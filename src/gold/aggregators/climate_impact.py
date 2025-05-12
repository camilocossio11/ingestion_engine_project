from pyspark.sql import functions as F

from .base import BaseAggregator


class ClimateImpact(BaseAggregator):
    """
    Aggregator class that analyzes the relationship between climate and sales.

    This class performs a join between climate data and online sales data at a daily level,
    and computes aggregated metrics to assess climate impact on sales.

    Aggregated metrics per day include:
        - Total sales amount (`total_sales`)
        - Average temperature in Celsius (`avg_temp_C`)
        - Average humidity percentage (`avg_humidity`)
    """

    def __init__(self, config_path):
        """
        Initialize the ClimateImpact aggregator.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    def aggregate(self):
        """
        Perform aggregation by joining online sales and climate data on the date.

        Steps:
        1. Read 'online_sales' and 'climate' tables from the silver layer.
        2. Extract the `date` from timestamp columns in both datasets.
        3. Join datasets on date and compute:
            - Total sales (sum of `sales_price`)
            - Average temperature (`temperature_C`)
            - Average humidity (`humidity_percent`)
        4. Write the aggregated results to the gold Delta table.
        """
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
