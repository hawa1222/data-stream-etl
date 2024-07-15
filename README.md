# Personal Data Stream ETL

This project is a Python-MySQL ETL pipeline to centralise data from multiple sources into a structured database, enabling advanced data analysis and application development. The project also includes the use of Redis for caching and S3 as a data lake in the extraction phase. Orchestration of the pipeline is done using Airflow.

## Project Objectives

- Establish a comprehensive personal data repository, laying the foundation for developing personalised applications and dashboards with the ability to query historical data.
- Utilise a range of technologies such as Python, MySQL, API integration, HTML scraping, Redis, S3, and Airflow.
- Implement diverse data extraction techniques such as API calls, manual exports, HTML scraping, and utilise Redis for caching and S3 as a data lake.
- Develop a modular pipeline for scalable processing.
- Integrate data from various formats including Excel, CSV, JSON, XML into distinct MySQL tables.

## Project Architecture

- **Extractors**: Modules to retrieve data from various sources, including Redis for caching and S3 as a data lake.
- **Transformers**: Modules to clean, standardise, and manipulate the data format.
- **Loader**: Modules responsible for inserting the data into the MySQL database.
- **Utility**: Contains helper functions and classes for database interactions (`DatabaseHandler`) and file management (`FileManager`).
- **Validation**: Script for post-load data validation to ensure data integrity and consistency.

### Data

- **Apple Health**: Includes datasets like walking metrics, daily activity, blood glucose levels, heart rate, fitness metrics, heart rate metrics, running metrics, sleep analysis, and steps.
- **Strava**: Sports activity data covering performance metrics, sport types, equipment information, and activity details.
- **YouTube**: Data related to YouTube likes/dislikes, subscriptions, etc.
- **Daylio**: Mood and activity tracking information.
- **Spend**: Financial data tracked over 6 years.

## Technologies Used

- Python (version 3.12)
- MySQL (version 8.3.0)
- Redis (version 6.0.0)
- S3 Bucket (AWS)
- Airflow (version 2.2.0)

## Setup Instructions

1. Clone the Repository:

    ```
    git clone https://github.com/hawa1222/data-stream-etl.git
    ```

2. Navigate to the project directory:

    ```
    cd data-stream-etl
    ```

3. Set up your environment:

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
