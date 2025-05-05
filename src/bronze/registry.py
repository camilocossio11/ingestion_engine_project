from ingestors.batch_ingestor import BatchIngestor
from ingestors.streaming_ingestor import StreamingIngestor

INGESTOR_REGISTRY = {"batch": BatchIngestor, "streaming": StreamingIngestor}
