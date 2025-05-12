from loguru import logger
from pyspark.sql import functions as F

from .base import BaseIngestor


class BatchIngestor(BaseIngestor):
    """
    Concrete implementation of BaseIngestor for batch ingestion.

    This class defines the `ingest` method to read streaming data from a
    landing zone, enrich it with metadata columns, and write it to a bronze
    Delta Lake table. It supports schema inference and checkpointing.

    Attributes:
        spark (SparkSession): Inherited Spark session from BaseIngestor.
        config (dict): Configuration loaded from the provided JSON file.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the BatchIngestor instance.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def ingest(self) -> None:
        """
        Perform the batch ingestion process:
            - Builds paths for the landing zone and bronze storage.
            - Reads streaming data using the format and options specified in the config.
            - Adds metadata columns such as `_ingested_filename` and `_ingestion_time`.
            - Writes the data as a Delta table in the bronze zone.
            - Waits for the streaming query to complete.
            - Creates the Delta table in the metastore if it does not exist.

        The configuration file must include:
            - storage_account_name
            - lakehouse_container_name
            - datasource
            - dataset
            - source: dict with keys `format` and `options`
            - sink: dict with keys `format` and `options`
        """
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
