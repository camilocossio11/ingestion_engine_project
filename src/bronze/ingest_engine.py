from loguru import logger
from registry import INGESTOR_REGISTRY


class Engine:

    def __init__(self, workload: str, config_path: str) -> None:
        self.workload = workload
        ingestor_class = INGESTOR_REGISTRY.get(workload)

        if not ingestor_class:
            error = f"No ingestor found for '{workload}' workload"
            logger.error(error)
            raise ValueError(error)

        self.ingestor = ingestor_class(config_path)

    def ingest(self) -> None:
        logger.info(f"Starting the ingestion for workload {self.workload}")
        self.ingestor.ingest()


if __name__ == "__main__":
    dataset = "climate"
    config_path = f"./config/bronze/{dataset}_config.json"
    workload = "batch"
    engine = Engine(workload, config_path)
    engine.ingest()
