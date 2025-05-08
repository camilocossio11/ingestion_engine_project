from loguru import logger
from pyspark.sql import functions as F

from .base import BaseIngestor


class BatchIngestor(BaseIngestor):

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)

    def ingest(self) -> None:
        logger.info("Defining configuration using config file")
        account = self.config.get("storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        lakehouse_container = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net"
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
