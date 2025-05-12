from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class ClimateProcessor(BaseProcessor):
    """
    Concrete processor class for handling climate-related datasets.

    This class extends the BaseProcessor to perform:
    - Timestamp normalization
    - Coordinate extraction (latitude, longitude)
    - Missing data imputation using moving averages

    The processing pipeline includes:
    1. Reading bronze data
    2. Applying defined transformations
    3. Performing imputations
    4. Writing the output to a silver Delta table
    5. Updating the last processed timestamp
    """

    def __init__(self, config_path: str):
        """
        Initialize the ClimateProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Apply data transformations:
        - Convert raw UNIX timestamps to timestamp format
        - Derive year, month, and day columns
        - Split coordinate column into Latitude and Longitude

        Args:
            df (DataFrame): Raw DataFrame from the bronze table.

        Returns:
            DataFrame: Transformed DataFrame with new timestamp and coordinate columns.
        """
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
        """
        Perform missing data imputations on specified fields using moving averages.

        Args:
            df (DataFrame): Transformed DataFrame with possible nulls.

        Returns:
            DataFrame: DataFrame with imputed values.
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
        """
        Execute the full processing pipeline:
        1. Read bronze data
        2. Apply transformations
        3. Apply imputations
        4. Write to silver table
        5. Update last processed date
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")

        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        df_imputed = self.imputations(df_transformed)
        self.write_delta_table(df_imputed)
        self.update_last_processed(df_imputed)
