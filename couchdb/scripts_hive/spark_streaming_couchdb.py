import os
import logging
import requests
import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

SEQUENCE_FILE = "last_seq.txt"
BATCH_SIZE = 500
COUCHDB_CHANGES_URL = "http://admin:root@localhost:5984/energy_consumption/_changes"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CouchDBToHiveStream")

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

def get_last_seq():
    return open(SEQUENCE_FILE).read().strip() if os.path.exists(SEQUENCE_FILE) else "0"

def save_last_seq(seq):
    with open(SEQUENCE_FILE, "w") as f:
        f.write(seq)

def fetch_changes(since):
    params = {
        "feed": "normal",
        "since": since,
        "include_docs": "true",
        "limit": BATCH_SIZE
    }
    try:
        response = requests.get(COUCHDB_CHANGES_URL, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Failed to fetch changes", exc_info=True)
        return None

def main():
    spark = SparkSession.builder \
        .appName("CouchDBToHiveStream") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE global_warming_db")

    last_seq = get_last_seq()

    while True:
        changes = fetch_changes(last_seq)
        if not changes or not changes.get("results"):
            time.sleep(5)
            continue

        docs = [r["doc"] for r in changes["results"] if r.get("doc")]
        rows = list(filter(None, map(transform_doc, docs)))

        if rows:
            df = spark.createDataFrame(rows, schema=schema)
            df.write.mode("append").insertInto("energy_consumptions")
            logger.info(f"Wrote {len(rows)} new rows to Hive.")

        last_seq = changes.get("last_seq", last_seq)
        save_last_seq(last_seq)

if __name__ == "__main__":
    main()
