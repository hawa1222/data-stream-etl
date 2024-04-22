# Personal Data Stream ETL

This project is a Python-MySQL ETL (Extract, Transform, Load) pipeline to centralise personal data from multiple sources into a structured database, enabling advanced data analysis and application development.

## Project Objectives

- Establish a comprehensive personal data repository, laying the foundation for developing personalised applications and dashboards with the ability to query historical data.
- Utilise a range of technologies such as Python, MySQL, API integration, HTML scraping.
- Implement diverse data extraction techniques such as API calls, manual exports, and HTML scraping. 
- Develop a modular pipeline for scalable processing.
- Integrate data from various formats including Excel, CSV, JSON, XML into distinct MySQL tables.


## Project Architecture

- **Extractors**: Modules to retrieve data from various sources.
- **Transformers**: Modules to clean, standardise, and manipulate the data format.
- **Loader**: Modules responsible for inserting the data into the MySQL database.
- **Utility**: Contains helper functions and classes for database interactions (`DatabaseHandler`) and file management (`FileManager`).
- **Validation**: Script for post-load data validation to ensure data integrity and consistency.

### Data

- **Apple Health**: Includes datasets like walking metrics, daily activity, blood glucose levels, heart rate, fitness metrics, low heart rate events, running metrics, sleep analysis, and steps.
- **Strava**: Sports activity data covering performance metrics, sport types, gear information, and activity details.
- **YouTube**: Data related to YouTube likes/dislikes, subscriptions, etc.
- **Daylio**: Mood and activity tracking information.
- **Spend**: Financial data personally tracked over 6 years.

## Prerequisites

Before running the Database API, ensure you have the following software installed:
- Python (version 3.12)
- MySQL (version 8.3.0)

## Setup Instructions

1. **Clone the Repository:**
   ```
   git clone https://github.com/hawa1222/personal_etl.git
   ```

2. **Navigate to the project directory:**
   ```
   cd personal_etl
   ```

3. **Set up your environment:**

   Make the setup script executable (if it's not already):

   ```
   chmod +x setup_environment.sh
   ```

   Then run the `setup_environment.sh` script to create a virtual environment and install all necessary packages. Execute this script from the root directory of the project:

   ```
   ./setup_environment.sh
   ```
4. Create a `.env` file in the project root directory and provide the environment variables as specified in `.env_template`.

## Usage

### Manual Execution

To execute the ETL process manually, run the following command in your terminal:
```
python main.py
```

### Automation

For automated execution, set up scheduling tasks on your operating system. This can be achieved through cron jobs on Unix-based systems or Task Scheduler on Windows.
