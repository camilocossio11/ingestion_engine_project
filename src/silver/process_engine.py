from loguru import logger

from silver.registry import PROCESSOR_REGISTRY


class ProcessorEngine:

    def __init__(self, dataset: str, config_path: str) -> None:
        self.dataset = dataset
        processor_class = PROCESSOR_REGISTRY.get(dataset)

        if not processor_class:
            error = f"No processor found for '{dataset}' dataset"
            logger.error(error)
            raise ValueError(error)

        self.processor = processor_class(config_path)

    def process(self) -> None:
        logger.info(f"Starting the ingestion for dataset {self.dataset}")
        self.processor.process()


if __name__ == "__main__":
    dataset = "climate"
    config_path = f"./config/silver/{dataset}_config.json"
    engine = ProcessorEngine(dataset, config_path)
    engine.process()
