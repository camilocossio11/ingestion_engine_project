from .ingestors import BatchIngestor, StreamingIngestor

INGESTOR_REGISTRY = {"batch": BatchIngestor, "streaming": StreamingIngestor}
