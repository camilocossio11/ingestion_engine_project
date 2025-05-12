from loguru import logger

from bronze.registry import INGESTOR_REGISTRY


class Engine:
    """
    Interface class for selecting and running the appropriate ingestor
    based on the specified workload.

    This class uses a registry of ingestors to dynamically instantiate and
    execute the corresponding ingestion logic defined by a subclass of `BaseIngestor`.

    Attributes:
        workload (str): The type of ingestion workload ('batch', 'streaming').
        ingestor (BaseIngestor): Instance of the appropriate ingestor class.
    """

    def __init__(self, workload: str, config_path: str) -> None:
        """
        Initialize the Engine and resolve the appropriate ingestor.

        Args:
            workload (str): The type of ingestion workload to run.
            config_path (str): Path to the configuration file to initialize the ingestor.
        """
        self.workload = workload
        ingestor_class = INGESTOR_REGISTRY.get(workload)

        if not ingestor_class:
            error = f"No ingestor found for '{workload}' workload"
            logger.error(error)
            raise ValueError(error)

        self.ingestor = ingestor_class(config_path)

    def ingest(self) -> None:
        """
        Execute the ingestion process using the resolved ingestor.
        """
        logger.info(f"Starting the ingestion for workload {self.workload}")
        self.ingestor.ingest()


if __name__ == "__main__":
    dataset = "climate"
    config_path = f"./config/bronze/{dataset}_config.json"
    workload = "batch"
    engine = Engine(workload, config_path)
    engine.ingest()
