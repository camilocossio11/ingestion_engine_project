from pyspark.sql import functions as F

from .base import BaseAggregator


class OrderFulfillmentPerformance(BaseAggregator):
    """
    Aggregator class to evaluate order fulfillment performance metrics.

    This class joins sales and logistics data to compute:
    - Delivery delay in days
    - On-time delivery rate
    - Total orders and number of on-time deliveries
    - Average delay per category and city

    The output is grouped by product category and delivery city and written
    to the gold layer as a Delta table.
    """

    def __init__(self, config_path):
        """
        Initialize the OrderFulfillmentPerformance aggregator.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def aggregate(self):
        """
        Perform aggregation to evaluate logistics fulfillment performance:

        1. Read `online_sales` and `logistics` tables from the silver layer.
        2. Join on `order_id`.
        3. Compute:
            - `delivery_delay_days`: difference between actual and scheduled delivery.
            - `on_time_delivery`: boolean flag for orders delivered on time.
        4. Group by `category` and `city` to calculate:
            - Total number of orders
            - Number of on-time deliveries
            - Average delivery delay
            - On-time delivery rate
        5. Write results to the gold Delta table.
        """
        sales_df = self.read_silver_table("online_sales")
        logistics_df = self.read_silver_table("logistics")

        order_fulfillment_df = (
            sales_df.alias("s")
            .join(logistics_df.alias("l"), on="order_id", how="inner")
            .withColumn(
                "delivery_delay_days", F.datediff("actual_delivery_date", "scheduled_delivery_date")
            )
            .withColumn(
                "on_time_delivery", F.when(F.col("delivery_delay_days") <= 0, True).otherwise(False)
            )
        )

        fulfillment_metrics = (
            order_fulfillment_df.groupBy("category", "city")
            .agg(
                F.count("*").alias("total_orders"),
                F.sum(F.col("on_time_delivery").cast("int")).alias("on_time_deliveries"),
                F.avg("delivery_delay_days").alias("avg_delay_days"),
            )
            .withColumn("on_time_rate", F.col("on_time_deliveries") / F.col("total_orders"))
        )

        self.write_delta_table(fulfillment_metrics)
