from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Batch Processing") \
    .config("spark.cassandra.connection.host", "cassandra-node") \
    .getOrCreate()


current_time = datetime.now()

end_time = current_time.replace(minute=0, second=0, microsecond=0)
start_time = end_time - timedelta(hours=1)

start_hour = start_time.hour
end_hour = end_time.hour


# Domain stats
page_time_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("table", "page_time_table") \
    .option("keyspace", "wiki_data") \
    .load() \
    .filter(hour("rev_timestamp") == start_hour)
    # .filter(hour("rev_timestamp") == start_hour+1)

domain_stats_df = page_time_df.groupBy("domain").agg(count("page_id").alias("created_pages"))

domain_stats_df = domain_stats_df.withColumn("start_hour", lit(start_time))
domain_stats_df = domain_stats_df.withColumn("end_hour", lit(end_time))

domain_stats_df = domain_stats_df.groupBy(["start_hour", "end_hour"]) \
                                 .agg(collect_list(struct("domain", "created_pages")).alias("statistics"))

domain_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "domain_stats") \
        .mode("append") \
        .save() 


# Domain bot stats
start_time = end_time - timedelta(hours=6)

start_hour = start_time.hour

page_bot_time_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("table", "page_time_table") \
    .option("keyspace", "wiki_data") \
    .load() \
    .filter((hour("rev_timestamp") >= start_hour) & (hour("rev_timestamp") < end_hour) & (col("user_is_bot") == True))
    # .filter((hour("rev_timestamp") >= start_hour) & (hour("rev_timestamp") <= end_hour) & (col("user_is_bot") == True))

domain_bot_stats_df = page_bot_time_df.groupBy("domain").agg(count("page_id").alias("created_pages"))

domain_bot_stats_df = domain_bot_stats_df.withColumn("start_hour", lit(start_time))
domain_bot_stats_df = domain_bot_stats_df.withColumn("end_hour", lit(end_time))

domain_bot_stats_df = domain_bot_stats_df.groupBy(["start_hour", "end_hour"]) \
                                 .agg(collect_list(struct("domain", "created_pages")).alias("statistics"))

domain_bot_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "domain_bot_stats") \
        .mode("append") \
        .save() 


# User stats
user_time_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("table", "user_time_table") \
    .option("keyspace", "wiki_data") \
    .load() \
    .filter((hour("rev_timestamp") >= start_hour) & (hour("rev_timestamp") < end_hour))
    # .filter((hour("rev_timestamp") >= start_hour) & (hour("rev_timestamp") <= end_hour))

user_stats_df = user_time_df.groupBy("user_id").agg(first("user_text").alias("user_name"),
                                                    count("page_id").cast("int").alias("created_pages"),
                                                    collect_list("page_title").alias("page_titles"))

user_stats_df = user_stats_df.orderBy(col("created_pages").desc()).limit(20)

user_stats_df = user_stats_df.withColumn("start_hour", lit(start_time))
user_stats_df = user_stats_df.withColumn("end_hour", lit(end_time))

user_stats_df = user_stats_df.groupBy(["start_hour", "end_hour"]) \
                             .agg(collect_list(struct("user_name", "user_id", "created_pages", "page_titles")).alias("statistics"))

user_stats_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_data") \
        .option("table", "user_stats") \
        .mode("append") \
        .save() 
