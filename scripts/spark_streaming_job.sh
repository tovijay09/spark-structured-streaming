#!/bin/bash
# Script to start the Spark Streaming Job

# Define the appropriate master URL
MASTER_URL="yarn url"
APP_NAME="KafkaToHDFS"
PYTHON_FILE="path/to/python_file.py"  # Change to relevant Python script

# Define other Spark configurations as required
# For example, you may want to set the maxRatePerPartition to avoid small files
# and adjust the number of cores and executors based on your cluster capacity

# Run the Spark Streaming Job
spark-submit --master $MASTER_URL \
             --name $APP_NAME \
             --deploy-mode cluster \
             --conf "spark.dynamicAllocation.enabled=true" \
             --conf "spark.streaming.kafka.maxRatePerPartition=100" \
             $PYTHON_FILE
