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


# Define the write function
def write_to_cassandra(batch_df, batch_id):

    # convert datetime
    batch_df = batch_df.withColumn("rev_timestamp", to_utc_timestamp("rev_timestamp", "UTC"))

    # domain table
    domain_df = batch_df.select(col("domain").cast("string").alias("domain"), 
                                col("page_id").cast("int").alias("page_id"))
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
    user_df = user_df.filter(user_df.user_id.isNotNull())

    if user_df.isEmpty():
        print("No valid rows to write to Cassandra.")
    else:
        user_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "wiki_data") \
            .option("table", "user_table") \
            .mode("append") \
            .save()    
    
    # user time table
    user_time_df = batch_df.select(col("user_id").cast("int").alias("user_id"),
                                   col("user_text").cast("string").alias("user_text"),
                                   col("page_id").cast("int").alias("page_id"),
                                   col("rev_timestamp").alias("rev_timestamp"))
    user_time_df = user_time_df.filter(user_time_df.user_id.isNotNull())

    if user_df.isEmpty():
        print("No valid rows to write to Cassandra.")
    else:
        user_time_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "wiki_data") \
            .option("table", "user_time_table") \
            .mode("append") \
            .save() 
    
    # page table
    page_df = batch_df.select(col("page_id").cast("int").alias("page_id"),
                              col("page_title").cast("string").alias("page_title"))
    page_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "page_table") \
        .mode("append") \
        .save() 
    
    # page stats table
    pages_stats_df = batch_df.select(col("rev_timestamp").alias("rev_timestamp"),
                                     col("domain").cast("string").alias("domain"),
                                     col("page_id").cast("int").alias("page_id"),
                                     col("user_is_bot").cast("boolean").alias("user_is_bot"))

    pages_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "pages_stats") \
        .mode("append") \
        .save() 
    
    # users stats
    users_stats_df = batch_df.select(col("rev_timestamp").alias("rev_timestamp"),
                                     col("user_id").cast("int").alias("user_id"),
                                     col("user_text").cast("string").alias("user_text"),
                                     col("page_id").cast("int").alias("page_id"))
    users_stats_df = users_stats_df.filter(users_stats_df.user_id.isNotNull())

    if user_df.isEmpty():
        print("No valid rows to write to Cassandra.")
    else:
        users_stats_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "wiki_data") \
            .option("table", "user_stats") \
            .mode("append") \
            .save() 


# Write the streaming DataFrame to Cassandra tables
df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .start() \
    .awaitTermination()
