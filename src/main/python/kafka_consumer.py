from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp, to_date
from pyspark.sql.types import StringType, StructType
from utils.xml_parser import parse_xml

# Define the XML schema, this will depend on your XML structure
xml_schema = StructType([...])

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaXMLConsumer").getOrCreate()

# Kafka Configuration
kafka_bootstrap_servers = "host1:port,host2:port"
kafka_topic_name = "your_topic"
kafka_group_id = "your_group_id"
kafka_offset_reset = "latest"  # "earliest" for non-prod

# Read from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", kafka_offset_reset) \
    .option("kafka.group.id", kafka_group_id) \
    .load()

# Selecting and casting the data as needed
kafka_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "offset")

# De-duplication using watermarking and dropDuplicates
# De-duplication can be handled by maintaining a list of processed offsets or using a primary key in the data, here we assume 'value' could be used
# This de-duplication strategy works within a single micro-batch. For cross micro-batch de-duplication, you would need an external storage to keep track of processed records.
deduplicated_df = kafka_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["key", "offset"])

# Parsing the XML data
# Define a UDF to parse XML to a StructType directly (utils.xml_parser )
parse_xml_udf = udf(lambda xml: parse_xml(xml), xml_schema)

# Apply the UDF to parse the XML string into a structured format
structured_df = deduplicated_df.withColumn("parsed_xml", parse_xml_udf(col("value")))

# Flatten the structured data
flattened_df = structured_df.select("parsed_xml.*")

# Dynamic Data Validation
# Schema validation - already enforced with `xml_schema`
# Data type validation - can be achieved by casting columns to the expected data types
# Data formatting - can be achieved by applying the `trim` function or similar operations
validated_df = flattened_df \
    .withColumn("trimmed_column", col("some_column").trim()) \
    .withColumn("timestamp", to_timestamp("date_field")) \
    .withColumn("casted_column", col("another_column").cast(StringType()))

# Fault Tolerance: Error Handling, Continuous Streaming, and Checkpointing
checkpoint_location = "/path/to/checkpoint/dir"

# Write the data to a HDFS location(raw_zone)
query = validated_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("checkpointLocation", checkpoint_location) \
        .option("path", "hdfs://path_to_raw_zone") \
        .partitionBy(to_date(col("date_field"))) \
        .start()

query.awaitTermination()

