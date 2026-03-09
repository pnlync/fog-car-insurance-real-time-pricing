from __future__ import annotations

import logging
import os

from common.models import AggregatedWindow

LOGGER = logging.getLogger("fog-publisher")


class BasePublisher:
    def publish(self, payload: AggregatedWindow) -> bool:
        raise NotImplementedError


class ConsolePublisher(BasePublisher):
    def publish(self, payload: AggregatedWindow) -> bool:
        LOGGER.info("Published payload: %s", payload.model_dump_json())
        return True


class AwsIotMqttPublisher(BasePublisher):
    def __init__(
        self,
        endpoint: str,
        client_id: str,
        ca_path: str,
        cert_path: str,
        key_path: str,
        port: int = 8883,
        topic_prefix: str = "insurance",
    ) -> None:
        import paho.mqtt.client as mqtt

        self.topic_prefix = topic_prefix.rstrip("/")
        self._mqtt = mqtt.Client(client_id=client_id)
        self._mqtt.tls_set(ca_certs=ca_path, certfile=cert_path, keyfile=key_path)
        self._mqtt.connect(endpoint, port=port)
        self._mqtt.loop_start()

    def publish(self, payload: AggregatedWindow) -> bool:
        topic = f"{self.topic_prefix}/{payload.vehicle_id}/{payload.trip_id}/telemetry"
        info = self._mqtt.publish(topic, payload.model_dump_json())
        info.wait_for_publish()
        return info.rc == 0


def build_publisher() -> BasePublisher:
    mode = os.getenv("MQTT_MODE", "console").lower()
    if mode == "console":
        return ConsolePublisher()
    if mode == "aws_iot":
        return AwsIotMqttPublisher(
            endpoint=os.environ["AWS_IOT_ENDPOINT"],
            client_id=os.getenv("AWS_IOT_CLIENT_ID", "fog-node"),
            ca_path=os.environ["AWS_IOT_CA_PATH"],
            cert_path=os.environ["AWS_IOT_CERT_PATH"],
            key_path=os.environ["AWS_IOT_KEY_PATH"],
            port=int(os.getenv("AWS_IOT_PORT", "8883")),
            topic_prefix=os.getenv("AWS_IOT_TOPIC_PREFIX", "insurance"),
        )
    raise ValueError(f"Unsupported MQTT_MODE={mode!r}")
