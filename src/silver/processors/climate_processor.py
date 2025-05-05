from loguru import logger
from processors.base import BaseProcessor
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F


class ClimateProcessor(BaseProcessor):

    def __init__(self, config_path: str):
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        logger.info("Performing transformations")
        transformations_config = self.config.get("transformations")
        coord_col = transformations_config.get("coord_col")
        raw_ts = self.config.get("raw_timestamp")

        df_tmstp = df.withColumn(
            "timestamp", F.from_unixtime(F.col(raw_ts) / 1000).cast("timestamp")
        )
        df = self.format_timestamp(df_tmstp)
        df_formatted = (
            df.withColumn("Latitude", F.col(coord_col).getItem(0).cast("double"))
            .withColumn("Longitude", F.col(coord_col).getItem(1).cast("double"))
            .drop(coord_col)
        )
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
                            F.col(field).isNull(), F.round(F.col(f"mean_last_{wdw_size_hr}h"), 3)
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
