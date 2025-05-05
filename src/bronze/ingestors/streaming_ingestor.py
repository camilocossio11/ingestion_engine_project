from confluent_kafka.schema_registry import SchemaRegistryClient
from ingestors.base import BaseIngestor
from loguru import logger
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


class StreamingIngestor(BaseIngestor):

    def __init__(self, config_path: str) -> None:
        super().__init__(config_path)

    @staticmethod
    def read_kafka_config(config_path: str):
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
