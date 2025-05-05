from loguru import logger
from processors.base import BaseProcessor
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F


class IoTProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        logger.info("Performing transformations")
        df_formatted = self.format_timestamp(df)
        return df_formatted

    def imputations(self, df: DF) -> DF:
        logger.info("Performing imputations")
        imputations_config = self.config.get("imputations")
        for imputation in imputations_config:
            field = imputation.get("field")
            logger.info(f"Imputing '{field}' field")
            imputation_method = imputation.get("method")
            if imputation_method == "window":
                wdw_size_hr = imputation.get("window_size_hr")
                partition_by = imputation.get("partition_by")
                df = (
                    self.compute_moving_average(df, field, wdw_size_hr, partition_by)
                    .withColumn(
                        field,
                        F.when(
                            F.isnan(F.col(field)), F.round(F.col(f"mean_last_{wdw_size_hr}h"), 3)
                        ).otherwise(F.col(field)),
                    )
                    .drop(f"mean_last_{wdw_size_hr}h", "event_time_long")
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
