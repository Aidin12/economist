#updated working_both polls and trends 

import pandas as pd
import logging
import numpy as np
from typing import Dict, List
from dataclasses import dataclass
import csv
import sys
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
import os 



# Setting up the logging framework
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Decorator to monitor function execution.
def monitor_function(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.error(f"Function {func.__name__} encountered an error: {e}")
            sys.exit(f"Critical error in function {func.__name__}. Exiting!")
    return wrapper


# ---- Data Integrity and Robustness Functions ----

def is_valid_date(date_string, format="%Y-%m-%d"):
    try:
        datetime.strptime(date_string, format)
        return True
    except ValueError:
        return False

def handle_candidate_dropouts(df: pd.DataFrame, default_columns: List[str]) -> pd.DataFrame:
    """
    Ensures dataframe has all expected columns. If a candidate drops out, fill their column with NaNs.
    
    Check if a column has no data for more than 14 recent days and fill the entire column with NaN.

    Args:
    - df: The dataframe to be processed.
    - default_columns: List of expected columns.

    Returns:
    - DataFrame after handling dropouts.

    Note:
    The function checks for more than 14 days of recent data absence, accounting for scenarios like Christmas 
    and 2-week holiday periods which can naturally have missing data.
    """

    # First, we'll check for columns that are entirely absent and fill them with NaNs.
    for col in default_columns:
        if col not in df.columns:
            df[col] = float('nan')
            logging.warning(f"Candidate {col} might have dropped out, column filled with NaN.")

    # Now, we'll check for columns that have missing data for more than 14 recent days.
    recent_data = df.tail(14)  # Getting the most recent 14 rows
    for col in df.columns:
        if recent_data[col].isna().sum() > 14:
            df[col] = float('nan')
            logging.warning(f"Candidate {col} might have dropped out recently, column filled with NaN.")

    return df


@monitor_function
def handle_multiple_polls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group polls by date and average them out if a pollster conducted 
    multiple polls on a given day.
    """
    try:
        # List of columns that should not be averaged
        non_averaged_columns = ["date", "pollster"]
        
        # Dynamically determine columns to aggregate by excluding non-averaged columns
        columns_to_aggregate = [col for col in df.columns if col not in non_averaged_columns]
        
        # Prepare a dictionary for aggregation with columns to average
        aggregation_dict = {}
        
        for col in columns_to_aggregate:
            try:
                # Attempt to convert the column to a numeric type
                df[col] = pd.to_numeric(df[col], errors='coerce')
                aggregation_dict[col] = "mean"
            except ValueError as ve:
                logging.warning(f"Column {col} couldn't be converted to numeric. Detail: {ve}. Skipping this column for aggregation.")
        
        # Group by date and compute the mean (average) for numeric columns
        df = df.groupby('date').agg(aggregation_dict).reset_index()
        
        return df

    except Exception as e:
        logging.error(f"Failed to handle multiple polls for date. Detail: {e}")
        return df





def split_date(df: pd.DataFrame) -> pd.DataFrame:
    """Split the date column into year, month, and day columns."""
    df['year'] = pd.to_datetime(df['date']).dt.year
    df['month'] = pd.to_datetime(df['date']).dt.month
    df['day'] = pd.to_datetime(df['date']).dt.day
    return df






def ensure_consistent_formatting(item: Dict) -> Dict:
    """Ensures consistent formatting of data. E.g., converting "30 %" to "30%". """
    for key, value in item.items():
        if isinstance(value, str):
            item[key] = value.replace(" %", "%").strip()
    return item

def _remove_outliers(self, df, candidates):
    for candidate in candidates:
        rolling_mean = df[candidate].rolling(window=7).mean()
        rolling_std = df[candidate].rolling(window=7).std()
        upper_bound = rolling_mean + (2 * rolling_std)

def remove_text_symbols(df: pd.DataFrame, columns_to_clean: List[str]) -> pd.DataFrame:
    """
    Remove all non-numeric symbols from specified columns except for the '.' which might be used for decimals.
    """
    for col in columns_to_clean:
        # Apply the cleaning only if the column exists in the dataframe
        if col in df.columns:
            # Use a lambda function to ensure non-numeric characters except for '.' are removed
            # We also ensure that the column name itself isn't being cleaned
            df[col] = df[col].apply(lambda x: ''.join(char for char in str(x) if char.isdigit() or char == '.') if x != col else x)
    return df




@dataclass
class PollEntry:
    date: str
    bulstrode: str
    lydgate: str
    vincy: str
    casaubon: str
    chettam: str
    others: str
    sample: int

# Decorator to monitor function execution.
def monitor_function(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.error(f"Function {func.__name__} encountered an error: {e}")
            # Sending alerts (e.g., email, system alert) can be integrated here.
            sys.exit(f"Critical error in function {func.__name__}. Exiting!")
    return wrapper


class PollsPipeline:
    """
    Pipeline to handle polling data processing, cleaning, and rolling average calculations.

    Attributes:
    ----------
    items : list
        List of polling data entries.

    Methods:
    -------
    process_item()
        converts date to datetime, save raw data for future calcs, remove %, output to polls.csv 
        verify data integrity
        error handling for missing data 

    remove_percentages():
        Removes percentage symbols from data.

    handle_missing_data():
        Handles any missing data in the dataframe.

    save_to_cleaned_csv():
        Saves cleaned data to 'cleaned.csv'.

    split_and_calculate_rolling_average():
        Splits date into year, month, day and calculates 7-day rolling averages.

    """

    def __init__(self):
        self.items = []
        self.polling_averages = []
        self.previous_averages = {}  # to monitor sudden opinion shifts
        self.scaler = MinMaxScaler()  # Initialize the MinMaxScaler for normalization

        self.default_columns = ["date", "pollster", "sample", "bulstrode", "lydgate", "vincy", "casaubon", "chettam", "others"]


    @monitor_function
    def normalize_data(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        for col in columns:
            try:
                # print(col)
                df[col] = pd.to_numeric(df[col], errors='coerce')
                valid_data = df[col].dropna()
                
                if valid_data.empty:
                    logging.warning(f"Column {col} contains only missing values. Skipping normalization for this column.")
                    continue

                # print('VALID DATA')
                # print(valid_data)
                # #valid_data = valid_data.astype(float)
                # print('converted')
          

                min_val = valid_data.min()
                max_val = valid_data.max()
                
                if min_val == max_val:
                    logging.warning(f"Column {col} contains constant values. Setting normalized value to 0 for this column.")
                    df[col] = 0
                    continue

                df[col] = (df[col] - min_val) / (max_val - min_val)
                #print(df)

            except Exception as e:
                logging.error(f"Error during normalization for column {col}. Exception: {e}")

        return df




    
    @monitor_function
    def process_item(self, item, spider):
        try:
            # Convert date to datetime format
            item["date"] = pd.to_datetime(item["date"]).strftime('%Y-%m-%d')
        except Exception as e:
            logging.error(f"Failed to transform date '{item.get('date','')}' to datetime. Detail: {e}")
            return

          # Remove percentages and normalize data
        for key in item:
            try:
                if "%" in str(item[key]):
                    item[key] = float(item[key].replace('%', ''))
            except Exception as e:
                logging.error(f"Error processing item for key '{key}' and value '{item[key]}'. Detail: {e}")
                return

        # Save raw data for later calculations
        self.items.append(item)

        return item



    
    
    def save_items_to_polls_csv(self):
        try:
            df = pd.DataFrame(self.items)

            candidates = [col for col in df.columns if col not in ["date", "pollster", "sample"]]

            # Handle multiple polls from the same pollster on the same day
            df = handle_multiple_polls(df)

            df = handle_candidate_dropouts(df, self.default_columns)

            # Normalize before saving or appending to polls.csv
            df = self.normalize_data(df, candidates)
            
            if os.path.exists('polls.csv'):
                df_existing = pd.read_csv('polls.csv')
                df = pd.concat([df_existing, df]).drop_duplicates().reset_index(drop=True)
                    
            df.to_csv('polls.csv', index=False)
        except Exception as e:
            logging.error(f"Failed to save data to 'polls.csv'. Detail: {e}")






    def calculate_rolling_average(self): 
            """
            📊 Calculate 7-day rolling averages for the specified columns.

            🔮🔮 Functions the Wizard created for us:
            1. Transforms our ✨magical✨ items into a pandas DataFrame.
            2. For each of our columns, excluding the 'date', 'pollster', and 'sample':
               a. Turns them into numeric values.
               b. Uses magic to fill in the gaps (NaN values).
               c. Casts a 7-day rolling average spell.
            3. Outputs resulting DataFrame with the rolling averages.

            📝 Note: for the first 6 days of 
            data, it'll read: NaN because it wants 7 full days to show its power.
            """
            
            # Converting our treasure trove of items into a DataFrame realm!
            df = pd.DataFrame(self.items)

            # Choosing our superhero columns!
            columns_to_average = [col for col in df.columns if col not in ["date", "pollster", "sample"]]

            for col in columns_to_average:
                try:
                    # Turning every item in the column into numeric. If it resists, turn it into NaN.
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # A little magic spell to fill in the gaps.
                    df[col] = df[col].interpolate(method='linear')  

                    # The grand 7-day rolling average spell! 🪄
                    df[col] = df[col].rolling(window=7).mean()

                except Exception as e:
                    # Logging with style and magic! 🌟
                    logging.error(f"🚨🚨🚨 Aw snap! Something's wonky in column '{col}'. "
                                  f"🧐 error: {e}. Review this column's data")

            # Showcasing our masterpiece!
            self.polling_averages = df





    def save_polling_averages(self):
        """Save the polling averages to trends.csv"""
        
        # Remove 'sample' column

        self.polling_averages.drop(columns=['sample'], inplace=True)
        
        # Save to trends.csv
        self.polling_averages.to_csv('trends.csv', index=False)


    @monitor_function
    def close_spider(self, spider):
        self.save_items_to_polls_csv()
        self.calculate_rolling_average()
        candidates = [col for col in self.polling_averages.columns if col not in ["date", "pollster", "sample"]]
        self.polling_averages = self.normalize_data(self.polling_averages, candidates)
        self.save_polling_averages()
