from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
import zmq

from common.models import SensorBatch

from .dataset_loader import load_behavior_rows
from .sensors import FleetConfig, FleetSimulator, SensorConfig

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("edge-simulator")


def load_config(config_path: str | Path) -> dict:
    with Path(config_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_fleet_config(raw_config: dict) -> FleetConfig:
    sensor_configs = {
        name: SensorConfig(**values) for name, values in raw_config["sensors"].items()
    }
    return FleetConfig(
        vehicle_count=int(raw_config["vehicles"]["count"]),
        interpolation_window_seconds=float(raw_config["interpolation_window_seconds"]),
        behavior_switch_probability=float(
            raw_config.get("behavior_switch_probability", 0.2)
        ),
        sensor_configs=sensor_configs,
    )


def main() -> None:
    config_path = os.getenv("EDGE_CONFIG_PATH", "edge/config.yaml")
    raw_config = load_config(config_path)
    grouped_rows = load_behavior_rows(raw_config["dataset_path"])
    simulator = FleetSimulator(build_fleet_config(raw_config), grouped_rows)

    context = zmq.Context.instance()
    socket = context.socket(zmq.PUSH)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(raw_config["zmq"]["endpoint"])
    LOGGER.info("Connected edge PUSH socket to %s", raw_config["zmq"]["endpoint"])

    dispatch_interval = timedelta(
        seconds=float(raw_config["dispatch_interval_seconds"])
    )
    tick_interval = 1 / float(raw_config["simulation_tick_hz"])
    batch_buffer = []
    next_dispatch = datetime.now(timezone.utc) + dispatch_interval

    try:
        while True:
            now = datetime.now(timezone.utc)
            batch_buffer.extend(simulator.collect_due_events(now))

            if now >= next_dispatch and batch_buffer:
                payload = SensorBatch(sent_at=now, events=batch_buffer)
                socket.send_json(json.loads(payload.model_dump_json()))
                LOGGER.info("Dispatched batch with %s sensor events", len(batch_buffer))
                batch_buffer = []
                next_dispatch = now + dispatch_interval

            time.sleep(tick_interval)
    except KeyboardInterrupt:
        LOGGER.info("Edge simulator stopped by user")
    finally:
        socket.close()


if __name__ == "__main__":
    main()
