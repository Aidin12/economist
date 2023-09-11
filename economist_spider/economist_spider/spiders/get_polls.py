import scrapy
from scrapy.exceptions import DropItem
from dataclasses import dataclass, asdict
from typing import List, Callable, Any
import functools
import logging
import csv 
import re 

# Decorator to handle missing extraction data.
def extraction_handler(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        result = func(*args, **kwargs)
        return result if result else "N/A"
    return wrapper

# Decorator to log the number of rows being processed.
def log_row_count(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(spider, response) -> Any:
        rows = response.xpath('//table//tr')
        print(f"Processing {len(rows)} rows from {response.url}")
        return func(spider, response)
    return wrapper

# Data structure using dataclass to represent polling data.
# Dataclass representing the structure of our polling data.
@dataclass
class PollData:
    date: str
    pollster: str
    sample: str
    bulstrode: str
    lydgate: str
    vincy: str
    casaubon: str
    chettam: str
    others: str

class PollingDataSpider(scrapy.Spider):
    name = 'get_polls'
    allowed_domains = ['cdn-dev.economistdatateam.com']
    start_urls = ['https://cdn-dev.economistdatateam.com/jobs/pds/code-test/index.html']

    # Helper method to extract data from a specific cell in the table.
        # This helper method abstracts the data extraction logic for individual table cells.
    # XPath is employed here to target specific cells in the HTML table.
    # In XPath:
    #   - `.//` begins the search from the current node.
    #   - `td` selects all <td> elements.
    #   - `[index]` acts as a positional filter, selecting the nth <td> element.
    #   - `/text()` gets the text content of the selected <td> element.

    @extraction_handler
    def extract_data(self, row, index):
        data = row.xpath(f'.//td[{index}]/text()').extract_first()
        if not data: 
            #if data extraction fails or results in empty data, a warning is logged
            #this aids to identify potential issues with the data source or logic of extraction 
            logging.warning(f"Failed to extract data from row {row} at position {index}")
        return data 



    # This method is where the scraping takes place.
    # It starts by selecting all rows of the table using XPath.
    # In XPath:
    #   - `//` selects nodes from the entire document.
    # The primary method where scraping happens.
    @log_row_count
    def parse(self, response) -> List[PollData]:
        rows = response.xpath('//table//tr')

        if not rows: 
            #raise an excepton if no rows are found in the table 
            #this can indicate a structural change in the webpage or a potential issue with the spider's logic 
            #DropItem is an exception raised when an item scraped from the spider is to be dropped 
            raise DropItem("No rows found in the table. The structure of the webpage might have changed")
        
        for row in rows:
            extracted_data = [self.extract_data(row, i+1) for i in range(9)]

            #if any key data (like 'date' or 'pollster' is missing, log a warning and skip the row. Then continue scraping)
            #for example, over Christmas or a two-week public holiday in June where there are big gaps in polling, the scraper will keep looping and not break
            if not extracted_data[0] or not extracted_data[1]:
                logging.warning(f"There is key data missing in row: {row}. We shall be skipping this row")
                continue 

            yield asdict(PollData(*extracted_data))

