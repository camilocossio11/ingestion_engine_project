from confluent_kafka.schema_registry import SchemaRegistryClient
from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from .base import BaseIngestor


class StreamingIngestor(BaseIngestor):
    """
    Concrete implementation of BaseIngestor for streaming ingestion from Kafka.

    This ingestor supports data encoded in Avro or JSON format, consumed from Kafka,
    optionally using Confluent Schema Registry for Avro schema resolution.

    The ingested data is enriched with metadata columns and written to a Delta Lake
    table in the bronze zone.

    Attributes:
        spark (SparkSession): Inherited Spark session from BaseIngestor.
        config (dict): Configuration loaded from the provided JSON file.
    """

    def __init__(self, config_path: str) -> None:
        """
        Initialize the StreamingIngestor instance.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        super().__init__(config_path)

    @staticmethod
    def read_kafka_config(config_path: str):
        """
        Read Kafka and Schema Registry configurations from a .properties-style config file.

        Args:
            config_path (str): Path to the Kafka config file.

        Returns:
            tuple:
                - kafka_spark_opts (dict): Options for Spark Kafka stream source.
                - schema_registry_config (dict): Configuration for Confluent Schema Registry.
        """
        config = {}
        with open(config_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split("=", 1)
                    config[parameter] = value.strip()

        username = config.get("sasl.username")
        password = config.get("sasl.password")
        kafka_spark_opts = {
            "kafka.bootstrap.servers": config.get("bootstrap.servers"),
            "kafka.security.protocol": config.get("security.protocol"),
            "kafka.sasl.mechanism": config.get("sasl.mechanisms"),
            "kafka.sasl.jaas.config": (
                f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{username}" '
                f'password="{password}";'
            ),
        }
        schema_registry_config = {
            "url": config.get("url"),
            "basic.auth.user.info": config.get("basic.auth.user.info"),
        }
        return kafka_spark_opts, schema_registry_config

    def avro_ingest(self):
        """
        Perform ingestion of Avro-encoded Kafka messages.

        Resolves the Avro schema from Confluent Schema Registry and decodes the Kafka
        message values using Spark's `from_avro` function.

        Returns:
            DataFrame: A streaming DataFrame with flattened Avro fields and metadata columns.
        """
        source = self.config.get("source")
        format = source.get("format")

        opts, schema_registry_config = StreamingIngestor.read_kafka_config(
            self.config.get("kafka_config_path")
        )
        opts.update(source.get("options"))

        topic = source.get("options").get("subscribe")
        value_subject = f"{topic}-value"
        schema_registry_client = SchemaRegistryClient(schema_registry_config)
        value_schema = schema_registry_client.get_latest_version(value_subject).schema.schema_str

        df = self.spark.readStream.format(format).options(**opts).load()
        df_renamed = df.toDF(*[f"_{col}" for col in df.columns])
        df_formatted = (
            df_renamed.withColumn("key", F.col("_key").cast("string"))
            .withColumn(
                "value", from_avro(F.expr("substring(_value,6,length(_value)-5)"), value_schema)
            )
            .withColumn("_ingestion_time", F.current_timestamp())
            .select("*", "value.*")
            .drop("value", "_key", "_value")
        )
        return df_formatted

    def json_ingest(self):
        """
        Perform ingestion of JSON-encoded Kafka messages.

        Parses the message values using a JSON schema provided in the config.

        Returns:
            DataFrame: A streaming DataFrame with parsed JSON fields and metadata columns.
        """
        source = self.config.get("source")
        format = source.get("format")
        json_schema = source.get("json_schema")

        opts, _ = StreamingIngestor.read_kafka_config(self.config.get("kafka_config_path"))
        opts.update(source.get("options"))

        df = self.spark.readStream.format(format).options(**opts).load()
        df_renamed = df.toDF(*[f"_{col}" for col in df.columns])

        df_formatted = (
            df_renamed.withColumn("key", F.col("_key").cast("string"))
            .withColumn("value", F.from_json(F.col("_value").cast("string"), json_schema))
            .withColumn("_ingestion_time", F.current_timestamp())
            .select("*", "value.*")
            .drop("value", "_key", "_value")
        )
        return df_formatted

    def ingest(self):
        """
        Perform the full ingestion process based on the configured value format:
            - Chooses ingestion strategy (`avro_ingest` or `json_ingest`).
            - Reads from Kafka topic as a streaming DataFrame.
            - Enriches with ingestion metadata.
            - Writes to a Delta Lake table in the bronze layer.
            - Waits for termination and registers the table if needed.
        """
        logger.info("Defining configuration using config file")
        source = self.config.get("source")
        value_format = source.get("value_format")

        if value_format == "avro":
            df = self.avro_ingest()
        elif value_format == "json":
            df = self.json_ingest()
        else:
            raise Exception(f"Invalid format {value_format}")

        account = self.config.get("storage_account_name")
        lkh_container_name = self.config.get("lakehouse_container_name")
        lakehouse_container = f"abfss://{lkh_container_name}@{account}.dfs.core.windows.net"
        bronze_path = f"{lakehouse_container}/bronze"
        datasource = self.config.get("datasource")
        dataset = self.config.get("dataset")

        sink = self.config.get("sink")
        sink_format = sink.get("format")
        sink_opts = sink.get("options")

        query = (
            df.writeStream.format(sink_format)
            .options(**sink_opts)
            .option("checkpointLocation", f"{bronze_path}/{datasource}/{dataset}_checkpoint/")
            .trigger(availableNow=True)
            .queryName(f"{datasource} {dataset}")
            .start(f"{bronze_path}/{datasource}/{dataset}")
        )
        query.awaitTermination()
        logger.info(
            f"Ingest process finished. Data written in {bronze_path}/{datasource}/{dataset}"
        )
        self.create_table(dataset, f"{bronze_path}/{datasource}/{dataset}")
