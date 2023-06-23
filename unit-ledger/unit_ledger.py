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
        config_topic: str,
        ads_b_input_topic: str,
        ais_input_topic: str,
        controller_topic: str,
        heartbeat_interval: float = 10.0,
        loop_sleep: float = 0.1,
        **kwargs: Any,
    ):
        """Instantiate the unit ledger by ...

        Parameters
        ----------
        config_topic: str
            MQTT topic for subscribing to config messages
        ads_b_input_topic: str
            MQTT topic for subscribing to ADS-B messages
        ais_input_topic: str
            MQTT topic for subscribing to AIS messages
        controller_topic: str
            MQTT topic for publishing controller messages
        heartbeat_interval: float
            Interval at which heartbeat message is to be published [s]
        loop_sleep: float
            Interval to sleep at end of main loop [s]

        Returns
        -------
        UnitLedger
        """
        # Parent class handles kwargs, including MQTT IP
        super().__init__(**kwargs)
        self.config_topic = config_topic
        self.ads_b_input_topic = ads_b_input_topic
        self.ais_input_topic = ais_input_topic
        self.controller_topic = controller_topic
        self.heartbeat_interval = heartbeat_interval
        self.loop_sleep = loop_sleep

        # Connect MQTT client
        logging.info("Connecting MQTT client")
        self.connect_client()
        sleep(1)
        self.publish_registration("Unit Ledger Module Registration")

        # Initialize ledger
        self.columns = [
            "unit_id",
            "time",
            "latitude",
            "longitude",
            "altitude",
            "track",
            "horizontal_velocity",
            "vertical_velocity",
        ]
        self.ledger = pd.DataFrame(columns=self.columns)
        self.ledger.set_index("unit_id", inplace=True)

        # Log configuration parameters
        logging.info(
            f"""UnitLedger initialized with parameters:
    config_topic = {config_topic}
    ads_b_input_topic = {ads_b_input_topic}
    ais_input_topic = {ais_input_topic}
    controller_topic = {controller_topic}
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
        config_topic = unit_ledger.get("config_topic", self.config_topic)
        ads_b_input_topic = unit_ledger.get("ads_b_input_topic", self.ads_b_input_topic)
        ais_input_topic = unit_ledger.get("ais_input_topic", self.ais_input_topic)
        controller_topic = unit_ledger.get("controller_topic", self.controller_topic)
        heartbeat_interval = unit_ledger.get(
            "heartbeat_interval", self.heartbeat_interval
        )
        loop_sleep = unit_ledger.get("loop_sleep", self.loop_sleep)

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
        # Populate and select state based on message type
        if type(msg) == mqtt.MQTTMessage:
            data = self.decode_payload(msg.payload)
        else:
            data = msg["data"]
        try:
            if "ADS-B" in data:
                logging.info(f"Processing ADS-B state message data: {data}")
                state = json.loads(data["ADS-B"])
                state["unit_id"] = state["icao"]

            elif "Decoded AIS" in data:
                logging.info(f"Processing AIS state message data: {data}")
                state = json.loads(data["Decoded AIS"])
                state["unit_id"] = state["mmsi"]
                # TODO: Sort out time value
                state["time"] = 0
                state["track"] = state["course"]

            else:
                logging.info(f"Skipping state message data: {data}")
                return

            selected_state = {}
            for column in self.columns:
                selected_state[column] = state[column]

        except:
            logging.error(f"Could not select state from state: {state}")

        # Add or update the state entry in the ledger
        unit_id = state["unit_id"]
        entry = pd.DataFrame(state, index=[unit_id])
        if entry.notna().all(axis=1).bool():
            if unit_id not in self.ledger.index:
                logging.info(f"Adding state message data for unit id: {unit_id}")
                self.ledger = pd.concat([self.ledger, entry], ignore_index=False)
            else:
                logging.info(f"Updating state message data for unit id: {unit_id}")
                self.ledger.update(entry)

        # TODO: Drop old entries

    # TODO: Complete
    def _select_unit(self: Any) -> None:
        pass

    def main(self: Any) -> None:
        """Schedule module heartbeat and ..."""
        # Schedule module heartbeat
        schedule.every(self.heartbeat_interval).seconds.do(
            self.publish_heartbeat, payload="Unit Ledger Module Heartbeat"
        )

        # TODO: Schedule unit selection

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
        config_topic=os.getenv("CONFIG_TOPIC", "TBC"),
        ads_b_input_topic=os.getenv("ADS_B_INPUT_TOPIC", "TBC"),
        ais_input_topic=os.getenv("AIS_INPUT_TOPIC", "TBC"),
        controller_topic=os.getenv("CONTROLLER_TOPIC", "TBC"),
        heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", 10.0)),
        loop_sleep=float(os.getenv("LOOP_SLEEP", 0.1)),
    )
    ledger.main()


if __name__ == "__main__":
    # Instantiate ledger and execute
    ledger = make_ledger()
    ledger.main()
