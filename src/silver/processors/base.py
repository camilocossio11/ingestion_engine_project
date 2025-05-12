import json
import os
from abc import ABC, abstractmethod

from loguru import logger
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F
from pyspark.sql.window import Window

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseProcessor(ABC):
    """
    Abstract base class for implementing data processing logic on Delta Lake tables
    using PySpark.

    This class provides utility methods for:
    - Reading data from a bronze table with optional incremental filtering
    - Enriching and transforming data
    - Writing processed data to a silver Delta table
    - Maintaining metadata such as last processed timestamps

    Subclasses must implement the `process` method to define specific transformation logic.

    Attributes:
        spark (SparkSession): Active Spark session for processing data.
        config (dict): Configuration dictionary loaded from a JSON file.
        config_path (str): Path to the configuration file, used for updating metadata.
    """

    def __init__(self, config_path: str):
        """
        Initialize the processor and load the configuration.

        Args:
            config_path (str): Path to the JSON config file.
        """
        self.spark = SparkSession.builder.getOrCreate()
        self.config_path = config_path

        with open(config_path, "r") as f:
            config = json.load(f)

        self.config = config
        self.connect_to_storage_account()

    @abstractmethod
    def process(self):
        """
        Abstract method to be implemented by subclasses to define the data
        processing workflow.
        """
        pass

    def connect_to_storage_account(self) -> None:
        """
        Configure Spark to access Azure Data Lake Storage using secrets
        stored in Databricks secret scope.
        """
        dbutils = DBUtils(self.spark)
        storage_account_name = self.config.get("storage_account_name")
        secret_scope = self.config.get("secret_scope")
        secret_key_name = self.config.get("secret_key_name")
        storage_account_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
        self.spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key
        )

    def read_bronze_table(self) -> DF:
        """
        Read data from the bronze table, optionally filtered by the last processed timestamp.

        Returns:
            DataFrame: The loaded data from the bronze layer.
        """
        last_ingest_processed = self.config.get("last_ingest_processed_date")
        source_config = self.config.get("source")
        schema = source_config.get("schema")
        table = source_config.get("table")

        if last_ingest_processed:
            logger.info(
                f"The last data processed were ingested on the date {last_ingest_processed}"
            )
            df = self.spark.table(f"{schema}.{table}").filter(
                F.col("_ingestion_time") > F.lit(last_ingest_processed).cast("timestamp")
            )
        else:
            logger.info(f"This is the first processing for table {schema}.{table}")
            df = self.spark.table(f"{schema}.{table}")

        timestamp_col = self.config.get("timestamp_col")
        if timestamp_col:
            df = df.withColumn(
                timestamp_col,
                F.when(F.col(timestamp_col).isNull(), F.current_timestamp()).otherwise(
                    F.col(timestamp_col)
                ),
            )

        logger.info("Bronze data read successfully")
        return df

    def uppercase_columns(self, df: DF, cols_to_upper: list) -> DF:
        """
        Uppercase the values of specific columns.

        Args:
            df (DataFrame): Input DataFrame.
            cols_to_upper (list): List of column names to transform.

        Returns:
            DataFrame: Updated DataFrame with uppercased columns.
        """
        for col in cols_to_upper:
            df = df.withColumn(col, F.upper(F.col(col)))
        return df

    def format_product(self, df: DF, col_name: str) -> DF:
        """
        Extract and format product-related information from a given column.

        Args:
            df (DataFrame): Input DataFrame.
            col_name (str): Column containing product info to parse.

        Returns:
            DataFrame: DataFrame with `product_id` and `product_name` columns.
        """
        logger.info("Formatting product field")
        pattern = r"([a-zA-Z]+_\d+)"
        df_formatted = (
            df.withColumn("no_sp_chars", F.regexp_replace(F.col(col_name), "[,-]", ""))
            .withColumn("product_id", F.upper(F.regexp_extract(F.col("no_sp_chars"), pattern, 1)))
            .withColumn(
                "product_name", F.upper(F.trim(F.regexp_replace(F.col("no_sp_chars"), pattern, "")))
            )
            .drop("no_sp_chars", col_name)
        )
        return df_formatted

    def format_timestamp(self, df: DF) -> DF:
        """
        Derive `year`, `month`, and `day` columns from a timestamp.

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with `year`, `month`, and `day` columns added.
        """
        timestamp_col = self.config.get("timestamp_col", "timestamp")
        df_formatted = (
            df.withColumn("year", F.year(F.col(timestamp_col)))
            .withColumn("month", F.month(F.col(timestamp_col)))
            .withColumn("day", F.dayofmonth(F.col(timestamp_col)))
        )
        return df_formatted

    def compute_moving_average(
        self, df: DF, field: str, wdw_size_hrs: int, partition_by: str
    ) -> DF:
        """
        Compute a moving average over a specified time window.

        Args:
            df (DataFrame): Input DataFrame.
            field (str): Field to average.
            wdw_size_hrs (int): Size of the moving window in hours.
            partition_by (str): Column to partition the window by.

        Returns:
            DataFrame: DataFrame with new column `mean_last_{wdw_size_hrs}h`.
        """
        seconds = wdw_size_hrs * 3600
        microseconds = seconds * 1_000_000

        timestamp_col = self.config.get("timestamp_col", "timestamp")
        df = df.withColumn("event_time_long", F.col(timestamp_col).cast("long") * 1_000_000)

        window_spec = (
            Window.orderBy("event_time_long")
            .partitionBy(partition_by)
            .rangeBetween(-microseconds, 0)
        )

        df = df.withColumn(f"mean_last_{wdw_size_hrs}h", F.avg(field).over(window_spec))
        return df

    def write_delta_table(self, df: DF) -> None:
        """
        Write the processed DataFrame to the silver Delta table.

        The table is partitioned by year, month, and day.

        Args:
            df (DataFrame): Processed DataFrame to write.
        """
        account = self.config.get("storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        silver_path = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net/silver"
        datasource = self.config.get("datasource")
        dataset = self.config.get("dataset")
        location = f"{silver_path}/{datasource}/{dataset}"

        sink_config = self.config.get("sink")
        schema = sink_config.get("schema")
        table = sink_config.get("table")
        logger.info(f"Uploading external table {schema}.{table} in '{location}'")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver MANAGED LOCATION '{silver_path}'")

        (
            df.write.format("delta")
            .mode("append")
            .partitionBy("year", "month", "day")
            .option("path", location)
            .saveAsTable(f"{schema}.{table}")
        )

    def update_last_processed(self, df: DF) -> None:
        """
        Update the `last_ingest_processed_date` in the config file with the latest
        `_ingestion_time` from the processed DataFrame.

        Args:
            df (DataFrame): DataFrame that was processed.
        """
        logger.info("Updating last processed data")
        if not df.isEmpty():
            max_timestamp_processed = df.select(F.max("_ingestion_time").cast("string")).first()[0]
            self.config["last_ingest_processed_date"] = max_timestamp_processed
            with open(self.config_path, "w") as f:
                json.dump(self.config, f, indent=4)
