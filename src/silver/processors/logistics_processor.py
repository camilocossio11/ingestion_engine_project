from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class LogisticsProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        transformations_config = self.config.get("transformations")
        coord_col = transformations_config.get("coord_col")
        cols_to_upper = transformations_config.get("columns_to_upper")
        df = self.format_timestamp(df)
        df_formatted = (
            df.withColumn("Latitude", F.col(coord_col).getItem(0))
            .withColumn("Longitude", F.col(coord_col).getItem(1))
            .drop(coord_col)
        )
        df_upper = self.uppercase_columns(df_formatted, cols_to_upper)
        return df_upper

    def imputations(self, df: DF) -> DF:
        logger.info("Performing imputations")
        imputations_config = self.config.get("imputations")
        for imputation in imputations_config:
            field = imputation.get("field")
            logger.info(f"Imputing '{field}' field")
            imputation_method = imputation.get("method")
            if imputation_method == "replace":
                col_value = imputation.get("col_value")
                df = df.withColumn(
                    field,
                    F.when(
                        (F.col(field).isNull()) & (F.col("shipment_status") == "RECEIVED"),
                        F.col(col_value),
                    ).otherwise(F.col(field)),
                )
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
