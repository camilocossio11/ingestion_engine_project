from aggregators.base import BaseAggregator
from pyspark.sql import functions as F


class SalesVolume(BaseAggregator):

    def __init__(self, config_path):
        super().__init__(config_path)

    def aggregate(self):
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
