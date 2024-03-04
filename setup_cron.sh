#!/bin/bash

# Path to your Python executable
PYTHON_PATH=/Users/hadid/anaconda3/envs/etlenv/bin/python

# Path to your main.py script
SCRIPT_PATH=/Users/hadid/Projects/ETL/main.py

# Setting up the cron job for monthly execution
(crontab -l 2>/dev/null; echo "0 0 1 * * $PYTHON_PATH $SCRIPT_PATH") | crontab -
echo "Cron job set up to run main.py on the 1st of every month at midnight"
