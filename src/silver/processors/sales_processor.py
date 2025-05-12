from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class SalesProcessor(BaseProcessor):
    """
    Concrete processor class for handling sales-related datasets.

    This processor applies multiple transformations to extract structured
    information from semi-structured fields like coordinates and addresses,
    and also applies simple fixed-value imputations for missing data.

    Processing Steps:
    1. Read bronze Delta table
    2. Format timestamp, coordinates, and address fields
    3. Extract product metadata
    4. Uppercase configured fields
    5. Apply fixed-value imputations
    6. Write to silver Delta table
    7. Update last processed timestamp
    """

    def __init__(self, config_path: str):
        """
        Initialize the SalesProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    @staticmethod
    def format_coords(df: DF, coord_col: str) -> DF:
        """
        Extract latitude and longitude from a raw string coordinate field.

        Args:
            df (DataFrame): Input DataFrame.
            coord_col (str): Name of the column containing coordinates.

        Returns:
            DataFrame: DataFrame with new `latitude` and `longitude` columns.
        """
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
        """
        Extract postal code from an address field and clean the address text.

        Args:
            df (DataFrame): Input DataFrame.
            address_col (str): Name of the column containing the address string.

        Returns:
            DataFrame: DataFrame with new `postal_code` and cleaned `address` columns.
        """
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
        """
        Apply all necessary transformations to raw sales data:
            - Convert timestamp
            - Format coordinates
            - Parse address and extract postal code
            - Extract product metadata
            - Uppercase configured columns

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Transformed DataFrame.
        """
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
        """
        Apply fixed-value imputations for missing fields.

        Args:
            df (DataFrame): Transformed DataFrame.

        Returns:
            DataFrame: DataFrame with imputed values.

        Raises:
            Exception: If an unsupported imputation method is provided.
        """
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
        """
        Run the full processing pipeline for sales data.

        Steps:
        1. Read from bronze layer
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
