from loguru import logger
from pyspark.sql import DataFrame as DF
from pyspark.sql import functions as F

from .base import BaseProcessor


class InventoryProcessor(BaseProcessor):
    """
    Concrete processor class for handling inventory-related datasets.

    This processor applies product-specific transformations and supports
    two types of imputation strategies:
    - Fixed offset from manufacturing date
    - Mean-based imputation per product ID

    Processing Steps:
    1. Read bronze Delta table
    2. Format and enrich product fields
    3. Apply uppercase transformations
    4. Impute missing values
    5. Write to silver Delta table
    6. Update last processed timestamp
    """

    def __init__(self, config_path: str):
        """
        Initialize the InventoryProcessor.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    @staticmethod
    def compute_mean(df: DF, field: str) -> DF:
        """
        Compute the mean of a numeric field grouped by product_id.

        Args:
            df (DataFrame): Input DataFrame.
            field (str): Name of the column to average.

        Returns:
            DataFrame: Joined DataFrame with new mean column.
        """
        df_mean = df.groupBy(F.col("product_id")).agg(F.avg(F.col(field)).alias(f"{field}_mean"))
        df_joined = df.join(df_mean, on="product_id", how="left")
        return df_joined

    def transformations(self, df: DF) -> DF:
        """
        Perform inventory-specific transformations:
        - Format product strings into product_id and product_name
        - Uppercase selected columns
        - Derive year/month/day from timestamps

        Args:
            df (DataFrame): Raw input DataFrame.

        Returns:
            DataFrame: Transformed DataFrame.
        """
        logger.info("Performing transformations")
        transformations_config = self.config.get("transformations")
        prod_col = transformations_config.get("prod_col")
        cols_to_upper = transformations_config.get("columns_to_upper")

        df = self.format_timestamp(df)
        df_formatted = self.format_product(df, prod_col)
        df_upper = self.uppercase_columns(df_formatted, cols_to_upper)
        return df_upper

    def imputations(self, df: DF) -> DF:
        """
        Perform missing value imputations using configured methods.

        Supported methods:
            - fixed: Offset from `manufacturing_date`
            - mean: Average value grouped by product_id

        Args:
            df (DataFrame): Transformed DataFrame.

        Returns:
            DataFrame: DataFrame with imputed fields.
        """
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
        """
        Run the full processing pipeline for inventory data.

        Steps:
        1. Read bronze data
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
