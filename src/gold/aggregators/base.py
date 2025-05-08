import json
import os
from abc import ABC, abstractmethod

from loguru import logger
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame as DF

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseAggregator(ABC):

    def __init__(self, config_path: str) -> None:
        self.spark = SparkSession.builder.getOrCreate()

        with open(config_path, "r") as f:
            config = json.load(f)

        self.config = config
        self.connect_to_storage_account()

    @abstractmethod
    def aggregate(self):
        pass

    def connect_to_storage_account(self):
        dbutils = DBUtils(self.spark)
        storage_account_name = self.config.get("storage_account_name")
        secret_scope = self.config.get("secret_scope")
        secret_key_name = self.config.get("secret_key_name")
        storage_account_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
        self.spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key
        )

    def read_silver_table(self, table_name) -> DF:
        logger.info(f"Reading table silver.{table_name}")
        df = self.spark.table(f"silver.{table_name}")

        logger.info("Data read successfully")
        return df

    def write_delta_table(self, df: DF) -> None:
        account = self.config.get("storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        gold_path = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net/gold"
        dataset = self.config.get("dataset")
        location = f"{gold_path}/{dataset}"

        sink_config = self.config.get("sink")
        schema = sink_config.get("schema")
        table = sink_config.get("table")
        logger.info(f"Uploading external table {schema}.{table} in '{location}'")

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold MANAGED LOCATION '{gold_path}'")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("path", location)
            .saveAsTable(f"{schema}.{table}")
        )
