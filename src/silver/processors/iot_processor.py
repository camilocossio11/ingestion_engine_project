from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class IoTProcessor(BaseProcessor):
    """
    Concrete processor class for handling IoT sensor datasets.

    This processor performs minimal transformations and applies imputation
    using a windowed moving average strategy, suitable for continuous numeric
    sensor data.

    Processing Steps:
    1. Read bronze Delta table
    2. Add timestamp-derived fields (year/month/day)
    3. Impute missing numeric values using time-window-based averages
    4. Write results to silver Delta table
    5. Update last processed timestamp
    """

    def __init__(self, config_path: str):
        """
        Initialize the IoTProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Add date-based columns (year, month, day) derived from a timestamp field.

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: Transformed DataFrame.
        """
        logger.info("Performing transformations")
        df_formatted = self.format_timestamp(df)
        return df_formatted

    def imputations(self, df: DF) -> DF:
        """
        Apply missing value imputations using window-based moving averages.

        The moving average is calculated per partition and time range.

        Args:
            df (DataFrame): Transformed DataFrame.

        Returns:
            DataFrame: Imputed DataFrame.
        """
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
        """
        Run the full processing pipeline for IoT sensor data.

        Steps:
        1. Read data from bronze layer
        2. Apply timestamp-based transformations
        3. Apply imputation logic
        4. Write data to the silver layer
        5. Update metadata on last processed record
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")

        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        df_imputed = self.imputations(df_transformed)
        self.write_delta_table(df_imputed)
        self.update_last_processed(df_imputed)
