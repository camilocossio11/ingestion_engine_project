from pyspark.sql import functions as F

from .base import BaseAggregator


class SalesVolume(BaseAggregator):
    """
    Aggregator class to compute daily sales volume metrics per product category.

    This class reads sales data from the silver layer and produces daily
    aggregations by category including:
    - Number of orders (`orders_count`)
    - Total sales (`total_sales`)
    - Average order value (`avg_order_value`)

    Results are written to the gold layer as a Delta table.
    """

    def __init__(self, config_path):
        """
        Initialize the SalesVolume aggregator.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def aggregate(self):
        """
        Perform aggregation to compute daily sales volume metrics by category.

        Steps:
        1. Read the `online_sales` table from the silver layer.
        2. Convert `timestamp` to `order_date`.
        3. Group by `category` and `order_date` to compute:
            - Number of orders (`orders_count`)
            - Total sales (`total_sales`)
            - Average order value (`avg_order_value`)
        4. Write the aggregated results to the gold Delta table.
        """
        sales_df = self.read_silver_table("online_sales")

        sales_by_category_df = (
            sales_df.withColumn("order_date", F.to_date("timestamp"))
            .groupBy("category", "order_date")
            .agg(
                F.count("*").alias("orders_count"),
                F.sum("sales_price").alias("total_sales"),
                F.avg("sales_price").alias("avg_order_value"),
            )
        )

        self.write_delta_table(sales_by_category_df)
