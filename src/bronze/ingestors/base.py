import json
import os
from abc import ABC, abstractmethod

from loguru import logger
from pyspark.dbutils import DBUtils

exec_env = os.getenv("EXECUTION_ENV", "local")
if exec_env == "databricks-connect":
    logger.info("Executing with databricks-connect")
    from databricks.connect import DatabricksSession as SparkSession
else:
    from pyspark.sql import SparkSession


class BaseIngestor(ABC):

    def __init__(self, config_path: str) -> None:
        self.spark = SparkSession.builder.getOrCreate()

        with open(config_path, "r") as f:
            config = json.load(f)

        self.config = config
        self.connect_to_storage_account()

    @abstractmethod
    def ingest(self):
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

    def create_table(self, dataset: str, location: str) -> None:
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
