from loguru import logger
from registry import INGESTOR_REGISTRY


class Engine:

    def __init__(self, workload: str, config_path: str):
        self.workload = workload
        ingestor_class = INGESTOR_REGISTRY.get(workload)

        if not ingestor_class:
            error = f"No ingestor found for '{workload}' workload"
            logger.error(error)
            raise ValueError(error)

        self.ingestor = ingestor_class(config_path)

    def ingest(self):
        logger.info(f"Starting the ingestion for workload {self.workload}")
        self.ingestor.ingest()


if __name__ == "__main__":
    dataset = "online_sales"
    config_path = f"./config/{dataset}_config.json"
    engine = Engine("batch", config_path)
    engine.ingest()
