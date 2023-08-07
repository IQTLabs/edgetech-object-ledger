"""Defines the ObjectLedger child class of BaseMQTTPubSub, and a
method for making ObjectLedger instances. Instatiates an ObjectLedger,
and executes its main() method when run as a module.
"""
import ast
from datetime import datetime
import json
import logging
import os
from time import sleep
import traceback
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
    level=os.environ.get("LOG_LEVEL", "INFO"),
    fmt="%(asctime)s.%(msecs)03d \033[0;90m%(levelname)-8s "
    ""
    "\033[0;36m%(filename)-18s%(lineno)3d\033[00m "
    "%(message)s",
    level_styles=STYLES,
)


class ObjectLedger(BaseMQTTPubSub):
    """Manage a ledger of moving objects, such as aircrafts or ships,
    that identifies the object, and provides its type and position.
    """

    def __init__(
        self,
        hostname: str,
        config_topic: str,
        ads_b_input_topic: str,
        ais_input_topic: str,
        ledger_output_topic: str,
        max_aircraft_entry_age: float = 60.0,
        max_ship_entry_age: float = 180.0,
        publish_interval: int = 1,
        heartbeat_interval: int = 10,
        loop_sleep: float = 0.1,
        continue_on_exception: bool = False,
        **kwargs: Any,
    ):
        """Instantiate the object ledger.

        Parameters
        ----------
        hostname (str): Name of host
        config_topic: str
            MQTT topic for subscribing to config messages
        ads_b_input_topic: str
            MQTT topic for subscribing to ADS-B messages
        ais_input_topic: str
            MQTT topic for subscribing to AIS messages
        ledger_output_topic: str,
            MQTT topic for publishing a message containing the full
            ledger
        max_aircraft_entry_age: float
            Maximum age of an aircraft entry in the ledger, after
            which it is dropped [s]
        max_ship_entry_age: float
            Maximum age of a ship entry in the ledger, after which it
            is dropped [s]
        publish_interval: int
            Interval at which the ledger message is published [s]
        heartbeat_interval: int
            Interval at which heartbeat message is published [s]
        loop_sleep: float
            Interval to sleep at end of main loop [s]
        continue_on_exception: bool
            Continue on unhandled exceptions if True, raise exception
            if False (the default)

        Returns
        -------
        ObjectLedger
        """
        # Parent class handles kwargs, including MQTT IP
        super().__init__(**kwargs)
        self.hostname = hostname
        self.config_topic = config_topic
        self.ads_b_input_topic = ads_b_input_topic
        self.ais_input_topic = ais_input_topic
        self.ledger_output_topic = ledger_output_topic
        self.max_aircraft_entry_age = max_aircraft_entry_age
        self.max_ship_entry_age = max_ship_entry_age
        self.publish_interval = publish_interval
        self.heartbeat_interval = heartbeat_interval
        self.loop_sleep = loop_sleep
        self.continue_on_exception = continue_on_exception

        # Connect MQTT client
        logging.info("Connecting MQTT client")
        self.connect_client()
        sleep(1)
        self.publish_registration("Object Ledger Module Registration")

        # Initialize ledger
        self.required_columns = [
            "object_id",
            "object_type",
            "timestamp",
            "latitude",
            "longitude",
            "altitude",
            "track",
            "horizontal_velocity",
            "vertical_velocity",
        ]
        self.ledger = pd.DataFrame(columns=self.required_columns)
        self.ledger.set_index("object_id", inplace=True)

        # Update max entry age dictionary
        self._set_max_entry_age()

        # Log configuration parameters
        self._log_config()

    def decode_payload(self, msg: Union[mqtt.MQTTMessage, str]) -> Dict[Any, Any]:
        """
        Decode the payload carried by a message.

        Parameters
        ----------
        payload: Union[mqtt.MQTTMessage, str]
            The MQTT message or payload string

        Returns
        -------
        Dict[Any, Any]
            The message data
        """
        if type(msg) == mqtt.MQTTMessage:
            payload = msg.payload.decode()
        else:
            payload = msg
        return json.loads(payload)

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
        # ignoring config message data without a "object-ledger" key
        data = self.decode_payload(msg)["Configuration"]
        if "object-ledger" not in data:
            return
        logging.info(f"Processing config message data: {data}")
        config = data["object-ledger"]
        self.hostname = config.get("hostname", self.hostname)
        self.config_topic = config.get("config_topic", self.config_topic)
        self.ads_b_input_topic = config.get(
            "ads_b_input_topic", self.ads_b_input_topic
        )
        self.ais_input_topic = config.get(
            "ais_input_topic", self.ais_input_topic
        )
        self.ledger_output_topic = config.get(
            "ledger_output_topic", self.ledger_output_topic
        )
        self.max_aircraft_entry_age = object_ledger.get(
            "max_aircraft_entry_age", self.max_aircraft_entry_age
        )
        self.max_ship_entry_age = object_ledger.get(
            "max_ship_entry_age", self.max_ship_entry_age
        )
        self.publish_interval = object_ledger.get(
            "publish_interval", self.publish_interval
        )
        self.heartbeat_interval = config.get(
            "heartbeat_interval", self.heartbeat_interval
        )
        self.loop_sleep = object_ledger.get("loop_sleep", self.loop_sleep)
        self.continue_on_exception = config.get(
            "continue_on_exception", self.continue_on_exception
        )

        # Update max entry age dictionary
        self._set_max_entry_age()

        # Log configuration parameters
        self._log_config()

    def _set_max_entry_age(self) -> None:
        """Populates maximum entry age dictionary.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.max_entry_age = {
            "aircraft": self.max_aircraft_entry_age,
            "ship": self.max_ship_entry_age,

    def _log_config(self: Any) -> None:
        """Logs all parameters that can be set on construction."""
        config = {
            "hostname": self.hostname,
            "config_topic": self.config_topic,
            "ads_b_input_topic": self.ads_b_input_topic,
            "ais_input_topic": self.ais_input_topic,
            "ledger_output_topic": self.ledger_output_topic,
            "max_aircraft_entry_age": self.max_aircraft_entry_age,
            "max_ship_entry_age": self.max_ship_entry_age,
            "publish_interval": self.publish_interval,
            "heartbeat_interval": self.heartbeat_interval,
            "loop_sleep": self.loop_sleep,
            "continue_on_exception": self.continue_on_exception,
        }
        logging.info(f"ObjectLedger configuration:\n{json.dumps(config, indent=4)}")


    def _get_max_entry_age(self, object_type: str) -> float:
        """Gets the maximum entry age based on object type.

        Parameters
        ----------
        object_type: str
            The object type: 'aircraft' or 'ship'

        Returns
        -------
        float
            The maximum entry age [s]
        """
        return self.max_entry_age[object_type]

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
        data = self.decode_payload(msg)
        if "ADS-B" in data:
            logging.info(f"Processing ADS-B state message data: {data}")
            state = json.loads(data["ADS-B"])
            state["object_id"] = state["icao_hex"]
            state["object_type"] = "aircraft"

        elif "Decoded AIS" in data:
            logging.info(f"Processing AIS state message data: {data}")
            state = json.loads(data["Decoded AIS"])
            state["object_id"] = state["mmsi"]
            state["object_type"] = "ship"
            state["track"] = state["course"]

        else:
            logging.info(f"Skipping state message data: {data}")
            return

        # Pop keys that are not required columns
        [state.pop(key) for key in set(state.keys()) - set(self.required_columns)]

        # Process required state
        entry = pd.DataFrame(state, index=[state["object_id"]])
        entry.set_index("object_id", inplace=True)
        if entry.notna().all(axis=1).bool():
            # Add or update the entry in the ledger
            if not entry.index.isin(self.ledger.index):
                logging.info(f"Adding entry state data for object id: {entry.index}")
                self.ledger = pd.concat([self.ledger, entry], ignore_index=False)

            else:
                logging.info(f"Updating entry state data for object id: {entry.index}")
                self.ledger.update(entry)

        else:
            logging.info(f"Invalid entry: {entry}")

    def _publish_ledger(self) -> None:
        """Drop ledger entries if their age exceeds the maximum
        specified by type, then publish the ledger.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Drop ledger entries if their age exceeds the maximum
        # specified by type
        index = self.ledger[
            datetime.utcnow().timestamp() - self.ledger["timestamp"]
            > self.ledger["object_type"].apply(self._get_max_entry_age)
        ]
        if not index.empty:
            logging.info(f"Dropping entry for object ids: {index}")
            self.ledger.drop(
                index,
                inplace=True,
            )

        # Send the full ledger to MQTT
        self._send_data(
            {
                "type": "ObjectLedger",
                "payload": self.ledger.to_json(),
            }
        )

    def _send_data(self, data: Dict[str, str]) -> bool:
        """Leverages edgetech-core functionality to publish a JSON
        payload to the MQTT broker on the topic specified in the class
        constructor.

        Parameters
        ----------
        data: Dict[str, str]
            Dictionary payload that maps keys to payload

        Returns
        -------
        bool
            Returns True if successful publish, else False
        """
        # TODO: Provide fields via environment or command line
        out_json = self.generate_payload_json(
            push_timestamp=int(datetime.utcnow().timestamp()),
            device_type="TBC",
            id_=self.hostname,
            deployment_id=f"TBC-{self.hostname}",
            current_location="TBC",
            status="Debug",
            message_type="Event",
            model_version="null",
            firmware_version="v0.0.0",
            data_payload_type=data["type"],
            data_payload=data["payload"],
        )
        success = self.publish_to_topic(self.ledger_output_topic, out_json)
        if success:
            logging.info(
                f"Successfully sent data on channel {self.ledger_output_topic}: {data}"
            )
        else:
            logging.info(
                f"Failed to send data on channel {self.ledger_output_topic}: {data}"
            )
        return success

    def main(self) -> None:
        """Schedule methods, subscribe to topics, and loop.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Schedule module heartbeat
        schedule.every(self.heartbeat_interval).seconds.do(
            self.publish_heartbeat, payload="Object Ledger Module Heartbeat"
        )

        # Schedule publishing of ledger message
        schedule.every(self.publish_interval).seconds.do(self._publish_ledger)

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
                logging.warning("Received keyboard interrupt")
                logging.warning("Exiting")
                sys.exit()

            except Exception as exception:
                # Optionally continue on exception
                if self.continue_on_exception:
                    traceback.print_exc()
                else:
                    raise


def make_ledger() -> ObjectLedger:
    """Construct an ObjectLedger instance.

    Parameters
    ----------
    None

    Returns
    -------
    ObjectLedger
        The ObjectLedger instance
    """
    return ObjectLedger(
        mqtt_ip=os.getenv("MQTT_IP", "mqtt"),
        hostname=os.environ.get("HOSTNAME", "TBC"),
        config_topic=os.getenv("CONFIG_TOPIC", "TBC"),
        ads_b_input_topic=os.getenv("ADS_B_INPUT_TOPIC", "TBC"),
        ais_input_topic=os.getenv("AIS_INPUT_TOPIC", "TBC"),
        ledger_output_topic=os.getenv("LEDGER_OUTPUT_TOPIC", "TBC"),
        max_aircraft_entry_age=float(os.getenv("MAX_AIRCRAFT_ENTRY_AGE", 60.0)),
        max_ship_entry_age=float(os.getenv("MAX_SHIP_ENTRY_AGE", 180.0)),
        publish_interval=int(os.getenv("PUBLISH_INTERVAL", 1)),
        heartbeat_interval=int(os.getenv("HEARTBEAT_INTERVAL", 10)),
        loop_sleep=float(os.getenv("LOOP_SLEEP", 0.1)),
        continue_on_exception=ast.literal_eval(
            os.environ.get("CONTINUE_ON_EXCEPTION", "False")
        ),
    )


if __name__ == "__main__":
    # Instantiate ObjectLedger and execute
    ledger = make_ledger()
    ledger.main()
