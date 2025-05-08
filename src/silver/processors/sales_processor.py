from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class SalesProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        super().__init__(config_path)

    @staticmethod
    def format_coords(df: DF, coord_col: str) -> DF:
        logger.info("Formatting coords")
        pattern = r"(\d+\.\d+).+(\d+\.\d+)"
        df_formatted = (
            df.withColumn("latitude_str", F.regexp_extract(F.col(coord_col), pattern, 1))
            .withColumn("longitude_str", F.regexp_extract(F.col(coord_col), pattern, 2))
            .withColumn("latitude", F.col("latitude_str").cast("double"))
            .withColumn("longitude", F.col("longitude_str").cast("double"))
            .drop("latitude_str", "longitude_str", coord_col)
        )
        return df_formatted

    @staticmethod
    def format_address(df: DF, address_col: str) -> DF:
        logger.info("Formatting addresses")
        pattern = r"\b(\d{5})\b"
        df_formatted = (
            df.withColumn("no_comma", F.regexp_replace(F.col(address_col), ",", ""))
            .withColumn("postal_code_str", F.regexp_extract(F.col("no_comma"), pattern, 1))
            .withColumn("postal_code", F.col("postal_code_str").cast("int"))
            .withColumn("address", F.trim(F.regexp_replace(F.col("no_comma"), pattern, "")))
            .drop("no_comma", "postal_code_str")
        )
        return df_formatted

    def transformations(self, df: DF) -> DF:
        logger.info("Performing transformations")
        transformations_config = self.config.get("transformations")
        coord_col = transformations_config.get("coord_col")
        address_col = transformations_config.get("address_col")
        prod_col = transformations_config.get("prod_col")
        cols_to_upper = transformations_config.get("columns_to_upper")

        df = self.format_timestamp(df)
        df_coords = SalesProcessor.format_coords(df, coord_col)
        df_address = SalesProcessor.format_address(df_coords, address_col)
        df_product = self.format_product(df_address, prod_col)
        df_upper = self.uppercase_columns(df_product, cols_to_upper)
        logger.info("Transformations finished")
        return df_upper

    def imputations(self, df: DF) -> DF:
        logger.info("Performing imputations")
        imputations_config = self.config.get("imputations")
        for imputation in imputations_config:
            field = imputation.get("field")
            logger.info(f"Imputing '{field}' field")
            imputation_method = imputation.get("method")
            if imputation_method == "fixed":
                df = df.fillna({field: imputation.get("value")})
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
