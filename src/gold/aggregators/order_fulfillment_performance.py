from pyspark.sql import functions as F

from .base import BaseAggregator


class OrderFulfillmentPerformance(BaseAggregator):

    def __init__(self, config_path):
        super().__init__(config_path)

    def aggregate(self):
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
