from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CouchDBBatchETL")

SEQUENCE_FILE = "last_seq.txt"

schema = StructType([
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("hydro_twh", DoubleType(), True),
    StructField("solar_twh", DoubleType(), True),
    StructField("wind_twh", DoubleType(), True),
    StructField("coal_ton", DoubleType(), True),
    StructField("gas_m3", DoubleType(), True),
    StructField("oil_m3", DoubleType(), True),
    StructField("nuclear_twh", DoubleType(), True),
    StructField("gas_emissions_ton", DoubleType(), True),
    StructField("population", IntegerType(), True),
    StructField("year", IntegerType(), True),
])

def clean_value(value, cast_type=float):
    try:
        if value in ["N/A", None]:
            return None
        return cast_type(value)
    except:
        return None

def transform_doc(doc):
    try:
        return Row(
            country=doc.get("country"),
            region=doc.get("region"),
            hydro_twh=clean_value(doc.get("renewable", {}).get("hydro_twh")),
            solar_twh=clean_value(doc.get("renewable", {}).get("solar_twh")),
            wind_twh=clean_value(doc.get("renewable", {}).get("wind_twh")),
            coal_ton=clean_value(doc.get("fossil", {}).get("coal_ton")),
            gas_m3=clean_value(doc.get("fossil", {}).get("gas_m3")),
            oil_m3=clean_value(doc.get("fossil", {}).get("oil_m3")),
            nuclear_twh=clean_value(doc.get("nuclear_twh")),
            gas_emissions_ton=clean_value(doc.get("gas_emissions_ton") or doc.get("gas_emmissions_ton")),
            population=clean_value(doc.get("population"), int),
            year=clean_value(doc.get("year"), int),
        )
    except Exception as e:
        logger.warning(f"Skipping invalid doc: {e}")
        return None

def fetch_all_docs():
    url = "http://admin:root@localhost:5984/energy_consumption/_all_docs?include_docs=true"
    try:
        res = requests.get(url)
        res.raise_for_status()
        data = res.json()
        return [row["doc"] for row in data["rows"] if "doc" in row]
    except Exception as e:
        logger.error("Failed to fetch documents from CouchDB", exc_info=True)
        return []
    
def fetch_last_sequence():
    url = "http://admin:root@localhost:5984/energy_consumption"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json().get("update_seq", "")
    except Exception as e:
        logger.error("Failed to fetch last sequence from CouchDB", exc_info=True)
        return ""


def main():
    spark = SparkSession.builder \
        .appName("CouchDBBatchETL") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE global_warming_db")

    docs = fetch_all_docs()
    logger.info(f"Fetched {len(docs)} documents from CouchDB")

    rows = list(filter(None, map(transform_doc, docs)))

    if not rows:
        logger.warning("No valid rows to write.")
        return

    df = spark.createDataFrame(rows, schema=schema)

    df.write.mode("append").insertInto("energy_consumptions")

    logger.info("Batch write to Hive completed.")

    last_seq = fetch_last_sequence()

    if last_seq:
        with open(SEQUENCE_FILE, "w") as f:
            f.write(last_seq)
        logger.info(f"Last sequence {last_seq} written to {SEQUENCE_FILE}")
    else:
        logger.warning("Could not retrieve last sequence.")


    spark.stop()

if __name__ == "__main__":
    main()
