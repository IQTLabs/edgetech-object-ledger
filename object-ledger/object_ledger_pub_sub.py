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
import threading
import traceback
from types import FrameType
from typing import Any, Dict, Optional, Union
import sys

import paho.mqtt.client as mqtt
import pandas as pd
import schedule

from base_mqtt_pub_sub import BaseMQTTPubSub


class ObjectLedgerPubSub(BaseMQTTPubSub):
    """Manage a ledger of moving objects, such as aircrafts or ships,
    that identifies the object, and provides its type and position.
    """

    def __init__(
        self,
        hostname: str,
        ads_b_topic: str,
        ais_json_topic: str,
        ledger_topic: str,
        max_aircraft_entry_age: float = 60.0,
        max_ship_entry_age: float = 180.0,
        publish_interval: int = 1,
        heartbeat_interval: int = 10,
        loop_interval: float = 0.001,
        log_level: str = "INFO",
        continue_on_exception: bool = False,
        **kwargs: Any,
    ):
        """Instantiate the object ledger.

        Parameters
        ----------
        hostname (str): Name of host
        ads_b_topic: str
            MQTT topic for subscribing to ADS-B messages
        ais_json_topic: str
            MQTT topic for subscribing to AIS JSON messages
        ledger_topic: str,
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
        loop_interval: int
            Interval during which main loop sleeps [s]
        log_level (str): One of 'NOTSET', 'DEBUG', 'INFO', 'WARN',
            'WARNING', 'ERROR', 'FATAL', 'CRITICAL'
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
        self.ads_b_topic = ads_b_topic
        self.ais_json_topic = ais_json_topic
        self.ledger_topic = ledger_topic
        self.max_aircraft_entry_age = max_aircraft_entry_age
        self.max_ship_entry_age = max_ship_entry_age
        self.publish_interval = publish_interval
        self.heartbeat_interval = heartbeat_interval
        self.loop_interval = loop_interval
        self.log_level = log_level
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
        self.exception = None
        self.state_lock = threading.Lock()

        # Update max entry age dictionary
        self._set_max_entry_age()

        # Log configuration parameters
        logging.info(
            f"""ObjectLedgerPubSub initialized with parameters:
    hostname = {hostname}
    ads_b_topic = {ads_b_topic}
    ais_json_topic = {ais_json_topic}
    ledger_topic = {ledger_topic}
    max_aircraft_entry_age = {max_aircraft_entry_age}
    max_ship_entry_age = {max_ship_entry_age}
    publish_interval = {publish_interval}
    heartbeat_interval = {heartbeat_interval}
    loop_interval = {loop_interval}
    log_level = {log_level}
    continue_on_exception = {continue_on_exception}
            """
        )

    def _decode_payload(self, msg: Union[mqtt.MQTTMessage, str]) -> Dict[Any, Any]:
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
        }

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
        try:
            logging.info("Entered ledger output callback")

            # Populate required state based on message type
            data = self._decode_payload(msg)
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

            # Acquire, then release a lock on the callback thread to
            # protect Pandas operations
            with self.state_lock:
                # Add or update the entry in the ledger
                entry = pd.DataFrame(state, index=[state["object_id"]])
                entry.set_index("object_id", inplace=True)
                if entry.notna().all(axis=1).bool():
                    if not entry.index.isin(self.ledger.index):
                        logging.info(f"Adding entry state data for object id: {entry.index}")
                        self.ledger = pd.concat([self.ledger, entry], ignore_index=False)

                    else:
                        logging.info(f"Updating entry state data for object id: {entry.index}")
                        self.ledger.update(entry)

                else:
                    logging.info(f"Invalid entry: {entry}")

        except Exception as exception:
            # Set exception
            self.exception = exception

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
        ].index
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
            },
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
        success = self.publish_to_topic(self.ledger_topic, out_json)
        if success:
            logging.info(
                f"Successfully sent data on channel {self.ledger_topic}: {data}"
            )
        else:
            logging.info(f"Failed to send data on channel {self.ledger_topic}: {data}")
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
        self.add_subscribe_topics(
            [self.ads_b_topic, self.ais_json_topic],
            [self._state_callback, self._state_callback],
            [2, 2],
        )

        # Enter the main loop
        while True:
            try:
                # Run pending scheduled messages
                schedule.run_pending()

                # Raise exception
                if self.exception is not None:
                    exception = self.exception
                    self.exception = None
                    raise exception

                # Prevent the loop from running at CPU time
                sleep(self.loop_interval)

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


if __name__ == "__main__":
    # Instantiate ObjectLedger and execute
    ledger = ObjectLedgerPubSub(
        mqtt_ip=os.getenv("MQTT_IP", "mqtt"),
        hostname=os.environ.get("HOSTNAME", ""),
        ads_b_topic=os.getenv("ADS_B_TOPIC", ""),
        ais_json_topic=os.getenv("AIS_JSON_TOPIC", ""),
        ledger_topic=os.getenv("LEDGER_TOPIC", ""),
        max_aircraft_entry_age=float(os.getenv("MAX_AIRCRAFT_ENTRY_AGE", 60.0)),
        max_ship_entry_age=float(os.getenv("MAX_SHIP_ENTRY_AGE", 180.0)),
        publish_interval=int(os.getenv("PUBLISH_INTERVAL", 1)),
        heartbeat_interval=int(os.getenv("HEARTBEAT_INTERVAL", 10)),
        loop_interval=float(os.getenv("LOOP_INTERVAL", 0.001)),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        continue_on_exception=ast.literal_eval(
            os.environ.get("CONTINUE_ON_EXCEPTION", "False")
        ),
    )
    ledger.main()
