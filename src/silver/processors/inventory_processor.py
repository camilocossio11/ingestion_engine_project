from loguru import logger
from processors.base import BaseProcessor
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F


class InventoryProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        super().__init__(config_path)

    @staticmethod
    def compute_mean(df: DF, field: str) -> DF:
        df_mean = df.groupBy(F.col("product_id")).agg(F.avg(F.col(field)).alias(f"{field}_mean"))
        df_joined = df.join(df_mean, on="product_id", how="left")
        return df_joined

    def transformations(self, df: DF) -> DF:
        logger.info("Performing transformations")
        transformations_config = self.config.get("transformations")
        prod_col = transformations_config.get("prod_col")
        cols_to_upper = transformations_config.get("columns_to_upper")

        df = self.format_timestamp(df)
        df_formatted = self.format_product(df, prod_col)
        df_upper = self.uppercase_columns(df_formatted, cols_to_upper)
        return df_upper

    def imputations(self, df: DF) -> DF:
        logger.info("Performing imputations")
        imputations_config = self.config.get("imputations")
        for imputation in imputations_config:
            field = imputation.get("field")
            logger.info(f"Imputing '{field}' field")
            imputation_method = imputation.get("method")
            if imputation_method == "fixed":
                df = df.withColumn(
                    field,
                    F.when(
                        F.col(field).isNull(),
                        F.date_add(F.col("manufacturing_date"), imputation.get("value")),
                    ).otherwise(F.col(field)),
                )
            elif imputation_method == "mean":
                df = InventoryProcessor.compute_mean(df, field)
                df = df.withColumn(
                    field,
                    F.when(F.col(field).isNull(), F.col(f"{field}_mean")).otherwise(F.col(field)),
                ).drop(f"{field}_mean")
            else:
                raise Exception(f"Imputation method '{imputation_method}' not configured")
        logger.info("Imputations finished")
        return df

    def process(self):
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")

        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        df_imputed = self.imputations(df_transformed)
        self.write_delta_table(df_imputed)
        self.update_last_processed(df_imputed)
