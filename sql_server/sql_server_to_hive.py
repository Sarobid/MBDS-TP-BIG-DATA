from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SQLServerToHive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE global_warming_db")

jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=NaturalCatastrophe;encrypt=true;trustServerCertificate=true"
properties = {
    "user": "sa",
    "password": "Admin_1_Admin",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

tables = ["Country","DisasterSubGroup","DisasterType","DisasterSubType","Region","Continent","Disaster"]

for table in tables:
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
    df.write.mode("overwrite").saveAsTable(table.lower())

spark.stop()