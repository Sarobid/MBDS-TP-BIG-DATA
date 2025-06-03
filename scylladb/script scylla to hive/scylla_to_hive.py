from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ScyllaToHive") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="policies", keyspace="climate_policies") \
    .load()

df.show()


df.write.mode("overwrite").parquet("climate_policies")

spark.stop()
