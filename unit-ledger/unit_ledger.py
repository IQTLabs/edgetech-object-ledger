from datetime import datetime
import json
import logging
import os
from time import sleep
from typing import Any, Dict, Union
import sys

import coloredlogs
import paho.mqtt.client as mqtt
import pandas as pd
import schedule

import axis_ptz_utilities
from base_mqtt_pub_sub import BaseMQTTPubSub

STYLES = {
    "critical": {"bold": True, "color": "red"},
    "debug": {"color": "green"},
    "error": {"color": "red"},
    "info": {"color": "white"},
    "notice": {"color": "magenta"},
    "spam": {"color": "green", "faint": True},
    "success": {"bold": True, "color": "green"},
    "verbose": {"color": "blue"},
    "warning": {"color": "yellow"},
}
coloredlogs.install(
    level=logging.INFO,
    fmt="%(asctime)s.%(msecs)03d \033[0;90m%(levelname)-8s "
    ""
    "\033[0;36m%(filename)-18s%(lineno)3d\033[00m "
    "%(message)s",
    level_styles=STYLES,
)


class UnitLedger(BaseMQTTPubSub):
    """Manage a ledger of moving objects, or units, such as aircraft
    or ships, that can be used for pointing a camera at the unit.
    """

    def __init__(
        self: Any,
        latitude_l: float,
        longitude_l: float,
        altitude_l: float,
        config_topic: str,
        ads_b_input_topic: str,
        ais_input_topic: str,
        controller_topic: str,
        max_age: float = 10.0,
        drop_interval: float = 10.0,
        select_interval: float = 2.0,
        heartbeat_interval: float = 10.0,
        loop_sleep: float = 0.1,
        **kwargs: Any,
    ):
        """Instantiate the unit ledger.

        Parameters
        ----------
        latitude_l: float
            Geodetic latitude of the unit ledger device [deg]
        longitude_l: float
            Longitude of the unit ledger device [deg]
        altitude_l: float
            Altitude of the unit ledger device [m]
        config_topic: str
            MQTT topic for subscribing to config messages
        ads_b_input_topic: str
            MQTT topic for subscribing to ADS-B messages
        ais_input_topic: str
            MQTT topic for subscribing to AIS messages
        controller_topic: str
            MQTT topic for publishing controller messages
        max_age: float
            Maximum age of ledger entries [minutes]
        drop_interval: float
            Interval at which ledger entries are dropped, if old [s]
        select_interval: float
            Interval at which units are selected for tracking [s]
        heartbeat_interval: float
            Interval at which heartbeat message is published [s]
        loop_sleep: float
            Interval to sleep at end of main loop [s]

        Returns
        -------
        UnitLedger
        """
        # Parent class handles kwargs, including MQTT IP
        super().__init__(**kwargs)
        self.latitude_l = latitude_l
        self.longitude_l = longitude_l
        self.altitude_l = altitude_l
        self.config_topic = config_topic
        self.ads_b_input_topic = ads_b_input_topic
        self.ais_input_topic = ais_input_topic
        self.controller_topic = controller_topic
        self.max_age = max_age
        self.drop_interval = drop_interval
        self.select_interval = select_interval
        self.heartbeat_interval = heartbeat_interval
        self.loop_sleep = loop_sleep

        # Compute position of the unit ledger device in the geocentric
        # (XYZ) coordinate system
        self.r_XYZ_l = axis_ptz_utilities.compute_r_XYZ(
            self.longitude_l, self.latitude_l, self.altitude_l
        )

        # Connect MQTT client
        logging.info("Connecting MQTT client")
        self.connect_client()
        sleep(1)
        self.publish_registration("Unit Ledger Module Registration")

        # Initialize ledger
        self.required_columns = [
            "unit_id",
            "unit_type",
            "timestamp",
            "latitude",
            "longitude",
            "altitude",
            "track",
            "horizontal_velocity",
            "vertical_velocity",
        ]
        self.computed_columns = [
            "distance",
        ]
        self.ledger = pd.DataFrame(
            columns=self.required_columns + self.computed_columns
        )
        self.ledger.set_index("unit_id", inplace=True)

        # Log configuration parameters
        logging.info(
            f"""UnitLedger initialized with parameters:
    latitude_l = {latitude_l}
    longitude_l = {longitude_l}
    altitude_l = {altitude_l}
    config_topic = {config_topic}
    ads_b_input_topic = {ads_b_input_topic}
    ais_input_topic = {ais_input_topic}
    controller_topic = {controller_topic}
    max_age = {max_age}
    drop_interval = {drop_interval}
    select_interval = {select_interval}
    heartbeat_interval = {heartbeat_interval}
    loop_sleep = {loop_sleep}
            """
        )

    def decode_payload(self, payload: mqtt.MQTTMessage) -> Dict[Any, Any]:
        """
        Decode the payload carried by a message.

        Parameters
        ----------
        payload: mqtt.MQTTMessage
            The MQTT message

        Returns
        -------
        data : Dict[Any, Any]
            The data component of the payload
        """
        # TODO: Establish and use message format convention
        content = json.loads(str(payload.decode("utf-8")))
        if "data" in content:
            data = content["data"]
        else:
            data = content
        return data

    def _config_callback(
        self,
        _client: Union[mqtt.Client, None],
        _userdata: Union[Dict[Any, Any], None],
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]],
    ) -> None:
        """
        Process config message.

        Parameters
        ----------
        _client: Union[mqtt.Client, None]
            MQTT client
        _userdata: Union[Dict[Any, Any], None]
            Any required user data
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]]
            An MQTT message, or dictionary

        Returns
        -------
        None
        """
        # Assign data attributes allowed to change during operation,
        # ignoring config message data without a "unit-ledger" key
        if type(msg) == mqtt.MQTTMessage:
            data = self.decode_payload(msg.payload)
        else:
            data = msg["data"]
        if "unit-ledger" not in data:
            return
        logging.info(f"Processing config message data: {data}")
        unit_ledger = data["unit-ledger"]
        self.latitude_l = unit_ledger.get("latitude_l", self.latitude_l)
        self.longitude_l = unit_ledger.get("longitude_l", self.longitude_l)
        self.altitude_l = unit_ledger.get("altitude_l", self.altitude_l)
        self.config_topic = unit_ledger.get("config_topic", self.config_topic)
        self.ads_b_input_topic = unit_ledger.get(
            "ads_b_input_topic", self.ads_b_input_topic
        )
        self.ais_input_topic = unit_ledger.get("ais_input_topic", self.ais_input_topic)
        self.controller_topic = unit_ledger.get(
            "controller_topic", self.controller_topic
        )
        self.max_age = unit_ledger.get("max_age", self.max_age)
        self.drop_interval = unit_ledger.get("drop_interval", self.drop_interval)
        self.select_interval = unit_ledger.get("select_interval", self.select_interval)
        self.heartbeat_interval = unit_ledger.get(
            "heartbeat_interval", self.heartbeat_interval
        )
        self.loop_sleep = unit_ledger.get("loop_sleep", self.loop_sleep)

    def _state_callback(
        self,
        _client: Union[mqtt.Client, None],
        _userdata: Union[Dict[Any, Any], None],
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]],
    ) -> None:
        """
        Process state message.

        Parameters
        ----------
        _client: Union[mqtt.Client, None]
            MQTT client
        _userdata: Union[Dict[Any, Any], None]
            Any required user data
        msg: Union[mqtt.MQTTMessage, Dict[Any, Any]]
            An MQTT message, or dictionary

        Returns
        -------
        None
        """
        # Populate required state based on message type
        if isinstance(msg, mqtt.MQTTMessage):
            data = self.decode_payload(msg.payload)
        else:
            data = msg["data"]
        try:
            if "ADS-B" in data:
                logging.info(f"Processing ADS-B state message data: {data}")
                state = json.loads(data["ADS-B"])
                state["unit_id"] = state["icao"]
                state["unit_type"] = "aircraft"

            elif "Decoded AIS" in data:
                logging.info(f"Processing AIS state message data: {data}")
                state = json.loads(data["Decoded AIS"])
                state["unit_id"] = state["mmsi"]
                state["unit_type"] = "ship"
                state["track"] = state["course"]

            else:
                logging.info(f"Skipping state message data: {data}")
                return

            # Pop keys that are not required columns
            [state.pop(key) for key in set(state.keys()) - set(self.required_columns)]

            # Initialize computed columns
            state["distance"] = 0.0

        except Exception as e:
            logging.error(f"Could not populate required state: {e}")

        # Process required state
        entry = pd.DataFrame(state, index=[state["unit_id"]])
        entry.set_index("unit_id", inplace=True)
        if entry.notna().all(axis=1).bool():
            # Compute position in the geocentric (XYZ) coordinate
            # system of the unit
            r_XYZ_u = axis_ptz_utilities.compute_r_XYZ(
                entry["longitude"].to_list()[0],
                entry["latitude"].to_list()[0],
                entry["altitude"].to_list()[0],
            )

            # Compute distance between the unit and the unit ledger
            # device
            entry["distance"] = axis_ptz_utilities.norm(r_XYZ_u - self.r_XYZ_l)

            # Add or update the entry in the ledger
            if not entry.index.isin(self.ledger.index):
                logging.info(f"Adding entry state data for unit id: {entry.index}")
                self.ledger = pd.concat([self.ledger, entry], ignore_index=False)

            else:
                logging.info(f"Updating entry state data for unit id: {entry.index}")
                self.ledger.update(entry)

        else:
            logging.info(f"Invalid entry: {entry}")

    def _drop_entries(self: Any) -> None:
        """Drop ledger entries if their age exceeds the maximum."""
        index = self.ledger[
            (datetime.utcnow().timestamp() - self.ledger["timestamp"]) / 60
            > self.max_age
        ].index
        logging.info(f"Dropping entry for unit ids: {index}")
        self.ledger.drop(
            index,
            inplace=True,
        )

    def _select_unit(self: Any) -> None:
        """Select the unit that is closest to the unit ledger device."""
        self.selected_unit_id = self.ledger["distance"].idxmin()
        logging.info(f"Selected unit with id: {self.selected_unit_id}")

    def main(self: Any) -> None:
        """Schedule methods, subscribe to required topics, and loop."""

        # Schedule module heartbeat
        schedule.every(self.heartbeat_interval).seconds.do(
            self.publish_heartbeat, payload="Unit Ledger Module Heartbeat"
        )

        # Schedule dropping of ledger entries after their age exceeds
        # the maximum
        schedule.every(self.drop_interval).seconds.do(self._drop_entries)

        # Schedule selection of a unit in the ledger for tracking
        schedule.every(self.select_interval).seconds.do(self._select_unit)

        # Subscribe to required topics
        self.add_subscribe_topic(self.config_topic, self._config_callback)
        self.add_subscribe_topic(self.ads_b_input_topic, self._state_callback)
        self.add_subscribe_topic(self.ais_input_topic, self._state_callback)

        # Enter the main loop
        while True:
            try:
                # Run pending scheduled messages
                schedule.run_pending()

                # Prevent the loop from running at CPU time
                sleep(self.loop_sleep)

            except KeyboardInterrupt as exception:
                # If keyboard interrupt, fail gracefully
                logging.debug(exception)
                sys.exit()

            except Exception as e:
                logging.error(f"Main loop exception: {e}")


def make_ledger() -> UnitLedger:
    return UnitLedger(
        mqtt_ip=os.getenv("MQTT_IP", "mqtt"),
        latitude_l=os.getenv("LATITUDE_L", 0.0),
        longitude_l=os.getenv("LONGITUDE_L", 0.0),
        altitude_l=os.getenv("ALTITUDE_L", 0.0),
        config_topic=os.getenv("CONFIG_TOPIC", "TBC"),
        ads_b_input_topic=os.getenv("ADS_B_INPUT_TOPIC", "TBC"),
        ais_input_topic=os.getenv("AIS_INPUT_TOPIC", "TBC"),
        controller_topic=os.getenv("CONTROLLER_TOPIC", "TBC"),
        max_age=float(os.getenv("MAX_AGE", 10.0)),
        drop_interval=float(os.getenv("DROP_INTERVAL", 10.0)),
        select_interval=float(os.getenv("SELECT_INTERVAL", 2.0)),
        heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", 10.0)),
        loop_sleep=float(os.getenv("LOOP_SLEEP", 0.1)),
    )
    ledger.main()


if __name__ == "__main__":
    # Instantiate ledger and execute
    ledger = make_ledger()
    ledger.main()
