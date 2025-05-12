from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class LogisticsProcessor(BaseProcessor):
    """
    Concrete processor class for handling logistics datasets.

    This processor handles:
    - Extraction of geographic coordinates (Latitude, Longitude)
    - Uppercasing of specified columns
    - Imputation based on business rules

    Processing Steps:
    1. Read bronze Delta table
    2. Apply transformations to enrich and standardize data
    3. Apply business-rule-based imputations
    4. Write results to silver Delta table
    5. Update last processed timestamp
    """

    def __init__(self, config_path: str):
        """
        Initialize the LogisticsProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def transformations(self, df: DF) -> DF:
        """
        Perform logistics-specific data transformations:
        - Extract latitude and longitude from a coordinate column
        - Uppercase specified columns
        - Add year/month/day from timestamp

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Transformed DataFrame.
        """
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
        """
        Perform business-rule-based imputations.

        - Method: "replace"
          Replace a null value with another column's value if shipment_status is "RECEIVED".

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
        """
        Execute the full processing pipeline for logistics data.

        Steps:
        1. Read from bronze table
        2. Apply transformations
        3. Apply imputations
        4. Write to silver Delta table
        5. Update last processed timestamp
        """
        logger.info(f"Starting processing of dataset {self.config.get('dataset')}")
        df = self.read_bronze_table()
        df_transformed = self.transformations(df)
        df_imputed = self.imputations(df_transformed)
        self.write_delta_table(df_imputed)
        self.update_last_processed(df_imputed)
