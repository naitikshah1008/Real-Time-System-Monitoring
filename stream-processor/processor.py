import json
import os

from anomaly import EWMADetector, TRACKED_METRICS
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import FlatMapFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("METRICS_RAW_TOPIC", "metrics_raw")
ANOMALY_TOPIC = os.getenv("METRICS_ANOMALY_TOPIC", "metrics_anomalies")
GROUP_ID = os.getenv("FLINK_GROUP_ID", "rtm-flink-anomaly")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "1"))
STARTING_OFFSET = os.getenv("FLINK_STARTING_OFFSET", "latest").lower()
REQUIRED_FIELDS = {"event_id", "host", "ts", *TRACKED_METRICS}


class AnomalyFlatMap(FlatMapFunction):
    def open(self, runtime_context):
        self.detector = EWMADetector()

    def flat_map(self, value):
        try:
            event = json.loads(value)
            if not REQUIRED_FIELDS.issubset(event):
                return
            for anomaly in self.detector.detect(event):
                yield json.dumps(anomaly)
        except Exception as e:
            print("Failed to process metric event:", e, value)


def starting_offsets():
    if STARTING_OFFSET == "earliest":
        return KafkaOffsetsInitializer.earliest()
    return KafkaOffsetsInitializer.latest()


def build_source():
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(RAW_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(starting_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_sink():
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(ANOMALY_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(serializer)
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    source = build_source()
    sink = build_sink()

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "metrics_raw")
        .flat_map(AnomalyFlatMap(), output_type=Types.STRING())
        .sink_to(sink)
    )
    env.execute("rtm-ai-advanced-anomaly-processor")


if __name__ == "__main__":
    main()
