import json
import os
import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer

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
HEALTH_PORT = int(os.getenv("PROCESSOR_HEALTH_PORT", "8090"))
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


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return

        body = json.dumps(
            {
                "status": "ok",
                "job": "rtm-ai-advanced-anomaly-processor",
                "tracked_metrics": list(TRACKED_METRICS),
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def start_health_server():
    if HEALTH_PORT <= 0:
        return

    def serve():
        server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
        print(f"Processor health endpoint listening on {HEALTH_PORT}")
        server.serve_forever()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()


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
    start_health_server()

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
