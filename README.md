# ETL Documentation

## ETL Pipeline Project Overview

Objective: Developed a Python-based ETL (Extract, Transform, Load) pipeline to centralise personal data from multiple sources into a MySQL database, aiming to enhance data accessibility for future application development.

Technologies Used: Python, MySQL, API integration, HTML scraping.

Key Features: Implemented diverse data extraction techniques such as API calls, manual exports, and HTML scraping. Developed a modular pipeline for scalable processing, and integrated data from various formats including Excel, CSV, JSON, XML into 19 distinct MySQL tables.

Outcome: Established a comprehensive personal data repository, laying the foundation for developing personalised applications and dashboards with the ability to query historical data.

## Project Summary

### Directory Structure

```plaintext
personal_etl/
├── __init__.py          # Marks directory as Python package directory.
├── .env                 # Environment variables file.
├── .git                 # Git repository metadata and tracking information.
├── .gitignore           # Lists files and directories ignored by Git.
├── archive              # Folder for archived items.
├── config.py            # Configuration settings for the project.
├── constants.py         # Constants used across the project.
├── credentials          # Stores sensitive credentials (ignored by Git).
├── data                 # Data files for the project.
├── documentation        # Project documentation.
├── exploratory_analysis # Scripts/notebooks for exploratory data analysis.
├── extractors           # Modules/scripts for data extraction.
├── loaders              # Modules for loading data into the system or database.
├── main.py              # Main script for running the project.
├── requirements.txt     # Project dependencies.
├── setup_cron.sh        # Shell script for setting up cron jobs.
├── tests                # Test scripts and test data.
├── transformers         # Modules for data transformation.
├── utility              # Utility scripts and helper functions.
├── validation           # Validation scripts or modules for data or inputs.
```

### Data

- **Apple Health**: Includes datasets like walking metrics, daily activity, blood glucose levels, heart rate, fitness metrics, low heart rate events, running metrics, sleep analysis, and steps.
- **Strava**: Sports activity data covering performance metrics, sport types, gear information, and activity details.
- **YouTube**: Data related to YouTube likes/dislikes, subscriptions, etc.
- **Daylio**: Mood and activity tracking information.
- **Spend**: Financial data personally tracked over 6 years.

### Key Components

- **Extractors**: Modules to retrieve data from various sources.
- **Transformers**: Modules to clean, standardise, and manipulate the data format.
- **Loader**: Modules responsible for inserting the data into the MySQL database.
- **Utility**: Contains helper functions and classes for database interactions (`DatabaseHandler`) and file management (`FileManager`).
- **Validation**: Script for post-load data validation to ensure data integrity and consistency.

## Usage

### Installation

1. **Clone the Repository**
   ```
   git clone https://github.com/hawa1222/personal_etl.git
   cd personal_etl
   ```

2. **Set up your environment**

   Make the setup script executable (if it's not already):

   ```
   chmod +x setup_environment.sh
   ```

   Then run the `setup_environment.sh` script to create a virtual environment and install all necessary packages. Execute this script from the root directory of the project:

   ```
   ./setup_environment.sh
   ```

### Manual Execution

To execute the ETL process manually, run the following command in your terminal:
```
python main.py
```

### Automation

For automated execution, set up scheduling tasks on your operating system. This can be achieved through cron jobs on Unix-based systems or Task Scheduler on Windows.
