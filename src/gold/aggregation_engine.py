from loguru import logger

from gold.registry import AGGREGATORS_REGISTRY


class AggregatorEngine:
    """
    Interface class for executing the appropriate data aggregation workflow.

    This engine dynamically selects and runs the aggregation logic registered
    under a given aggregation name via the `AGGREGATORS_REGISTRY`.

    Attributes:
        aggregation (str): The name of the aggregation task (e.g., 'climate_impact').
        aggregator (BaseAggregator): An instance of a concrete aggregator class.
    """

    def __init__(self, aggregation: str, config_path: str) -> None:
        """
        Initialize the AggregatorEngine with the appropriate aggregator.

        Args:
            aggregation (str): Name of the aggregation task (must be registered).
            config_path (str): Path to the configuration file for the aggregator.
        """
        self.aggregation = aggregation
        aggregator_class = AGGREGATORS_REGISTRY.get(aggregation)

        if not aggregator_class:
            error = f"No aggregation '{aggregation}' found"
            logger.error(error)
            raise ValueError(error)

        self.aggregator = aggregator_class(config_path)

    def aggregate(self) -> None:
        """
        Run the aggregation logic using the selected aggregator.
        """
        logger.info(f"Starting aggregation '{self.aggregation}'")
        self.aggregator.aggregate()


if __name__ == "__main__":
    aggregation = "sales_volume"
    config_path = f"./config/gold/{aggregation}_config.json"
    engine = AggregatorEngine(aggregation, config_path)
    engine.aggregate()
