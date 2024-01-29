#!/bin/bash
# Script to start the Hourly Batch Processing Job

# Define the appropriate master URL
MASTER_URL="yarn url"
APP_NAME="batch_processor_job"
PYTHON_FILE="path/to/python_file.py"  


# Run the Spark Batch Job
spark-submit --master $MASTER_URL \
             --name $APP_NAME \
             --deploy-mode cluster \
             $PYTHON_FILE