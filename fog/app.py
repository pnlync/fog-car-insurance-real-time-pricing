from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
import zmq

from common.models import SensorBatch
from common.pricing import RiskThresholds

from .buffer import LocalSpool
from .mqtt_publisher import build_publisher
from .processor import FogProcessor, ProcessorConfig

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("fog-node")


def load_config(config_path: str | Path) -> dict:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_processor(config: dict) -> FogProcessor:
    thresholds = RiskThresholds(**config["risk_thresholds"])
    processor_config = ProcessorConfig(
        window_seconds=int(config["window_seconds"]),
        harsh_brake_threshold=float(config["harsh_brake_threshold"]),
        lane_departure_threshold=float(config["lane_departure_threshold"]),
        emit_lag_seconds=float(config.get("emit_lag_seconds", 2.0)),
        thresholds=thresholds,
    )
    return FogProcessor(processor_config)


def main() -> None:
    config_path = os.getenv("FOG_CONFIG_PATH", "fog/config.yaml")
    config = load_config(config_path)
    processor = build_processor(config)
    publisher = build_publisher()
    spool = LocalSpool(config["spool_path"])

    context = zmq.Context.instance()
    socket = context.socket(zmq.PULL)
    socket.setsockopt(zmq.LINGER, 0)
    socket.bind(config["zmq"]["bind"])
    socket.setsockopt(zmq.RCVTIMEO, int(config.get("receive_timeout_ms", 1000)))
    LOGGER.info("Fog PULL socket bound to %s", config["zmq"]["bind"])

    try:
        while True:
            try:
                payload = socket.recv_json()
                batch = SensorBatch.model_validate(payload)
                processor.consume_batch(batch)
                LOGGER.info("Received batch with %s events", len(batch.events))
            except zmq.error.Again:
                pass

            replayed = spool.replay(publisher.publish)
            if replayed:
                LOGGER.info("Replayed %s buffered MQTT payloads", replayed)

            ready_windows = processor.flush_ready(datetime.now(timezone.utc))
            for window in ready_windows:
                if not publisher.publish(window):
                    LOGGER.warning(
                        "Publish failed for vehicle=%s trip=%s, spooling locally",
                        window.vehicle_id,
                        window.trip_id,
                    )
                    spool.append(window)
    except KeyboardInterrupt:
        LOGGER.info("Fog node stopped by user")
    finally:
        socket.close()


if __name__ == "__main__":
    main()
