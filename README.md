# economist
Nidia's Spider To Scrape Polling Data

This spider scrapes data from the `polling_economist` site. It's designed to be run in its own virtual environment to ensure compatibility and dependency separation.

## Prerequisites

- Make sure you have Python installed.
- Git must be installed if you're cloning the repository.

## Setup

1. Clone the repository:

```bash
git clone <repository_url>
```

2. Navigate to the repository folder:

```bash
cd polling_economist
```

## Running the Spider

To run the spider in just one command, execute:

```bash
./run_spider.sh
```

Note: Ensure the script `run_spider.sh` is executable. If not, make it executable by running `chmod +x run_spider.sh`.

### Inside run_spider.sh

If you're wondering what's inside the `run_spider.sh`, here's a typical structure:

```bash
#!/bin/bash

# Activate the virtual environment
source env/bin/activate

# Run the scrapy spider
scrapy crawl spider_name (replace spider_name placement with the name of spider: get_polls)

# Deactivate the virtual environment after the spider finishes
deactivate
```

Make sure to replace `spider_name` with the actual name of your spider.

---

This README assumes that:
1. The environment folder is named `env`.
2. The spider can be run using `scrapy crawl spider_name`.
3. The `run_spider.sh` script is in the root of the `polling_economist` folder.

Adjust the instructions as needed based on the actual structure and requirements of your project.**README.md for polling_economist**

---

# Polling Economist

**polling_economist** is a web scraping spider designed to scrape polling data and process it using various data cleaning and analysis techniques. This repository contains all the required code to run and monitor the spider, as well as handling the obtained data.

---

## Table of Contents
1. [Summary](#Summary)
2. [Requirements](#Requirements)
3. [Installation](#Installation)
4. [How to Run](#How-to-Run)
5. [Features & Functions](#Features-&-Functions)
6. [License](#License)
7. [Author](#Author)

---

### Summary

The spider aims to scrape polling data and process it for better insights. The pipeline includes handling missing data, calculating rolling averages, normalization, and saving the results to structured CSV files. It also has error handling and logging features to monitor its process.

---

### Requirements

- Python 3.x
- pandas library
- logging module
- numpy library
- datetime module
- scikit-learn for preprocessing
- os module

---

### Installation

1. Clone the repository to your local machine.
   
   ```bash
   git clone <repository_url>
   ```

2. Navigate to the **polling_economist** directory.

   ```bash
   cd polling_economist
   ```

3. Install the required libraries and dependencies using the provided environment.

   ```bash
   source env/bin/activate
   pip install -r requirements.txt
   ```

---

### How to Run

Once you have installed all dependencies, you can run the spider with a single command:

```bash
python main.py  # Assuming the main driver script is named main.py
```

---

### Features & Functions

1. **Data Integrity and Robustness Functions** - Functions that ensure the data's quality and handle errors or missing values.

2. **Data Cleaning Functions** - Functions to handle multiple polls, split dates, ensure consistent formatting, remove text symbols, and normalize data.

3. **Pipeline for Polling Data** - A pipeline to manage the process of polling data including removing percentage symbols, handling missing data, saving to CSV, and calculating rolling averages.

4. **Logging and Monitoring** - Comprehensive logging setup and decorators to monitor function execution and errors.

---

### License

This project is licensed under the MIT License. See the [LICENSE.md](LICENSE.md) file for details.

---

### Author

Nidia Sahjara



---

Happy scraping and data processing! 🕷📊🤖

--- 

**Note**: Remember to replace placeholder text like `<repository_url>`, `[Your Name]`, and `[youremail@example.com]` with the appropriate details.
