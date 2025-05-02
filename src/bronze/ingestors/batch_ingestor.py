import json
import os

from ingestors.base import BaseIngestor
from loguru import logger
from pyspark.sql import functions as F

exec_env = os.getenv("EXECUTION_ENV", "databricks-connect")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BatchIngestor(BaseIngestor):

    def __init__(self, config_path: str):
        self.spark = SparkSession.builder.getOrCreate()

        with open(config_path, "r") as f:
            config = json.load(f)

        self.config = config
        self.connect_to_storage_account(self.spark, self.config.get("storage_account_name"))

    def create_table(self, dataset, location):
        if not self.spark.catalog.tableExists(f"bronze.{dataset}"):
            logger.info(f"Creating external table bronze.{dataset}")
            self.spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS bronze.{dataset}
                USING DELTA
                LOCATION '{location}'"""
            )
            logger.info("Table successfully created")

    def ingest(self) -> None:
        logger.info("Defining configuration using config file")
        account = self.config.get("storage_account_name")
        lakehouse_container = f"abfss://datalake@{account}.dfs.core.windows.net"
        landing_container = f"abfss://landing@{account}.dfs.core.windows.net"

        datasource = self.config.get("datasource")
        dataset = self.config.get("dataset")

        bronze_path = f"{lakehouse_container}/bronze"
        landing_path = f"{landing_container}/{datasource}/{dataset}"
        logger.info(f"Data read from {landing_path}")

        source = self.config.get("source")
        source_format = source.get("format")
        source_opts = source.get("options")

        sink = self.config.get("sink")
        sink_format = sink.get("format")
        sink_opts = sink.get("options")
        logger.info("Starting ingest process")

        df = (
            self.spark.readStream.format(source_format)
            .options(**source_opts)
            .option("cloudFiles.schemaLocation", f"{bronze_path}/{datasource}/{dataset}_schema")
            .load(landing_path)
            .withColumn("_ingested_filename", F.input_file_name())
            .withColumn("_ingestion_time", F.current_timestamp())
        )
        query = (
            df.writeStream.format(sink_format)
            .options(**sink_opts)
            .option("checkpointLocation", f"{bronze_path}/{datasource}/{dataset}_checkpoint/")
            .trigger(availableNow=True)
            .queryName(f"{datasource} {dataset}")
            .start(f"{bronze_path}/{datasource}/{dataset}")
        )
        query.awaitTermination()
        logger.info(
            f"Ingest process finished. Data written in {bronze_path}/{datasource}/{dataset}"
        )
        self.create_table(dataset, f"{bronze_path}/{datasource}/{dataset}")
