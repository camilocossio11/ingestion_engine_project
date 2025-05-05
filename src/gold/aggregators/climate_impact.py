from aggregators.base import BaseAggregator
from pyspark.sql import functions as F


class ClimateImpact(BaseAggregator):

    def __init__(self, config_path):
        super().__init__(config_path)

    def aggregate(self):
        sales_df = self.read_silver_table("online_sales")
        climate_df = self.read_silver_table("climate")

        orders_by_day = sales_df.withColumn("date", F.to_date("timestamp"))
        climate_by_day = climate_df.withColumn("date", F.to_date("timestamp"))

        sales_climate_df = (
            orders_by_day.alias("o")
            .join(climate_by_day.alias("c"), on="date", how="inner")
            .groupBy("date")
            .agg(
                F.sum("sales_price").alias("total_sales"),
                F.avg("temperature_C").alias("avg_temp_C"),
                F.avg("humidity_percent").alias("avg_humidity"),
            )
        )

        self.write_delta_table(sales_climate_df)
