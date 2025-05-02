from abc import ABC, abstractmethod

from pyspark.dbutils import DBUtils


class BaseIngestor(ABC):

    @abstractmethod
    def ingest(self):
        pass

    def connect_to_storage_account(self, spark, storage_account_name):
        dbutils = DBUtils(spark)
        storage_account_key = dbutils.secrets.get(scope="akvmde01ccossi01", key="accesskey")
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key
        )
