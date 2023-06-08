from pyspark.sql import SparkSession


# Create a SparkSession
spark = SparkSession.builder \
    .appName("CassandraReadExample") \
    .config("spark.cassandra.connection.host", "cassandra-node") \
    .getOrCreate()


# Read data from Cassandra
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("table", "domain_table") \
    .option("keyspace", "wiki_data") \
    .load()


# Display the results
df.show(10)
print(df.count())

# Start with cassandra:
# spark-submit --conf spark.jars.ivy=/opt/app --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0" --master spark://spark-batch-master:7077 --deploy-mode client batch_processor.py