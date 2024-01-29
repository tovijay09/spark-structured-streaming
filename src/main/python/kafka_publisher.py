from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def publish_to_kafka(source_df: DataFrame):
    source_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host1:port,host2:port") \
        .option("topic", "kafka_topic_name") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaPublisher").getOrCreate()
    
    # Read data from source
    df = spark.read.load("path_to_source")

    # Publish to Kafka
    publish_to_kafka(df)