from loguru import logger
from registry import AGGREGATORS_REGISTRY


class AggregatorEngine:

    def __init__(self, aggregation: str, config_path: str) -> None:
        self.aggregation = aggregation
        aggregator_class = AGGREGATORS_REGISTRY.get(aggregation)

        if not aggregator_class:
            error = f"No aggregation '{aggregation}' found"
            logger.error(error)
            raise ValueError(error)

        self.aggregator = aggregator_class(config_path)

    def aggregate(self) -> None:
        logger.info(f"Starting aggregation '{self.aggregation}'")
        self.aggregator.aggregate()


if __name__ == "__main__":
    aggregation = "sales_volume"
    config_path = f"./config/gold/{aggregation}_config.json"
    engine = AggregatorEngine(aggregation, config_path)
    engine.aggregate()
