from pyspark.sql import SparkSession
from pyspark.sql.streaming import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

# Define the schema for the JSON
schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", IntegerType(), True)
    ]), True),
    StructField("database", StringType(), True),
    StructField("page_id", IntegerType(), True),
    StructField("page_title", StringType(), True),
    StructField("page_namespace", IntegerType(), True),
    StructField("rev_id", IntegerType(), True),
    StructField("rev_timestamp", StringType(), True),
    StructField("rev_sha1", StringType(), True),
    StructField("rev_minor_edit", BooleanType(), True),
    StructField("rev_len", IntegerType(), True),
    StructField("rev_content_model", StringType(), True),
    StructField("rev_content_format", StringType(), True),
    StructField("performer", StructType([
        StructField("user_text", StringType(), True),
        StructField("user_groups", ArrayType(StringType()), True),
        StructField("user_is_bot", BooleanType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_registration_dt", StringType(), True),
        StructField("user_edit_count", IntegerType(), True)
    ]), True),
    StructField("page_is_redirect", BooleanType(), True),
    StructField("comment", StringType(), True),
    StructField("parsedcomment", StringType(), True),
    StructField("rev_slots", StructType([
        StructField("main", StructType([
            StructField("rev_slot_content_model", StringType(), True),
            StructField("rev_slot_sha1", StringType(), True),
            StructField("rev_slot_size", IntegerType(), True),
            StructField("rev_slot_origin_rev_id", IntegerType(), True)
        ]), True)
    ]), True)
])


spark = SparkSession \
        .builder \
        .appName("Spark Kafka Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.streaming.checkpointLocation", "/opt/app/spark-checkpoint") \
        .config("spark.cassandra.connection.host", "cassandra-node") \
        .getOrCreate()


kafka_bootstrap_servers = "kafka:9092"
input_topic_name = "source-data"
output_topic_name = "output-data"


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false")\
    .load()

df = df.selectExpr("CAST(value AS STRING)").withColumn("value", from_json(col("value"), schema))

df = df.select(col("value.meta.domain").alias("domain"), \
               col("value.performer.user_id").alias("user_id"), \
               col("value.performer.user_text").alias("user_text"), \
               col("value.performer.user_is_bot").alias("user_is_bot"), \
               col("value.page_id").alias("page_id"), \
               col("value.page_title").alias("page_title"), \
               col("value.rev_timestamp").alias("rev_timestamp"), \
               col("value").alias("value"))

# df.select(col("domain").alias("value")).writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
#     .option("topic", output_topic_name) \
#     .option("checkpointLocation", "/opt/app/kafka_checkpoint")\
#     .start().awaitTermination()

# query = df.select(col("page_title").alias("domain")).writeStream\
#  .option("checkpointLocation", '/opt/app/cassandra_checkpoint')\
#  .format("org.apache.spark.sql.cassandra")\
#  .option("keyspace", "project")\
#  .option("table", "domains")\
#  .start().awaitTermination()

# Define the write function
def write_to_cassandra(batch_df, batch_id):

    # domain table
    domain_df = batch_df.select(col("domain").cast("string").alias("domain"), col("page_id").cast("int").alias("page_id"))
    domain_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "domain_table") \
        .mode("append") \
        .save()
    
    # user table
    user_df = batch_df.select(col("user_id").cast("int").alias("user_id"),
                              col("page_id").cast("int").alias("page_id"),
                              col("page_title").cast("string").alias("page_title"))
    user_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "user_table") \
        .mode("append") \
        .save()    
    
    # user time table
    # user_time_df = batch_df.select(col("user_id").cast("int").alias("user_id"),
    #                                col("user_text").cast("text").alias("user_text"),
    #                                col("page_id").cast("int").alias("page_id"),
    #                                col("rev_timestamp").cast("string").alias("rev_timestamp"))
    # user_time_df.write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .option("keyspace", "wiki_data") \
    #     .option("table", "user_table") \
    #     .mode("append") \
    #     .save() 

    # batch_df.select(col("page_title").alias("page_title")).write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .option("keyspace", "wiki_data")\
    #     .option("table", "page_titles")\
    #     .mode("append") \
    #     .save()

    # batch_df.select(col("domain").alias("domain")).write \
    #     .format("org.apache.spark.sql.cassandra") \
    #     .option("keyspace", "wiki_data")\
    #     .option("table", "domains")\
    #     .mode("append") \
    #     .save()
    
# Write the streaming DataFrame to Cassandra tables
df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .start() \
    .awaitTermination()

# docker run --rm -it --network project-network --name spark-submit -v /home/yromanu/UCU/third_year/second_term/big-data-ucu-project/spark_stream:/opt/app bitnami/spark:3 /bin/bash

# spark-submit --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0" --master spark://spark-master:7077 --deploy-mode client stream_processor.py

# Start with cassandra:
# spark-submit --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0" --master spark://spark-master:7077 --deploy-mode client stream_processor.py

# kafka producer
# docker run -it --rm --network project-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-console-producer.sh --broker-list kafka:9092 --topic source-data

# kafka consumer
# docker run -it --rm --network project-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic output-data
