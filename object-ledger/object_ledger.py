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


class ObjectLedger(BaseMQTTPubSub):
    """Manage a ledger of moving objects, such as aircrafts or ships,
    that can be used for pointing a camera at the object.
    """

    def __init__(
        self: Any,
        hostname: str,
        latitude_l: float,
        longitude_l: float,
        altitude_l: float,
        config_topic: str,
        ads_b_input_topic: str,
        ais_input_topic: str,
        ledger_output_topic: str,
        selection_output_topic: str,
        max_age: float = 10.0,
        max_aircraft_track_interval: float = 4.0,
        max_ship_track_interval: float = 1.0,
        drop_interval: float = 10.0,
        select_interval: float = 2.0,
        heartbeat_interval: float = 10.0,
        loop_sleep: float = 0.1,
        **kwargs: Any,
    ):
        """Instantiate the object ledger.

        Parameters
        ----------
        hostname (str): Name of host
        latitude_l: float
            Geodetic latitude of the object ledger device [deg]
        longitude_l: float
            Longitude of the object ledger device [deg]
        altitude_l: float
            Altitude of the object ledger device [m]
        config_topic: str
            MQTT topic for subscribing to config messages
        ads_b_input_topic: str
            MQTT topic for subscribing to ADS-B messages
        ais_input_topic: str
            MQTT topic for subscribing to AIS messages
        ledger_output_topic: str,
            MQTT topic for publishing a message containing the full
            ledger
        selection_output_topic: str,
            MQTT topic for publishing a message containing the
            selected object
        max_age: float
            Maximum age of ledger entries [minutes]
        max_aircraft_track_interval: float
            Maximum interval in which aircraft are selected for
            tracking [minutes]
        max_ship_track_interval: float
            Maximum interval in which ship are selected for tracking
            [minutes]
        drop_interval: float
            Interval at which ledger entries are dropped, if old [s]
        select_interval: float
            Interval at which objects are selected for tracking [s]
        heartbeat_interval: float
            Interval at which heartbeat message is published [s]
        loop_sleep: float
            Interval to sleep at end of main loop [s]

        Returns
        -------
        ObjectLedger
        """
        # Parent class handles kwargs, including MQTT IP
        super().__init__(**kwargs)
        self.hostname = hostname
        self.latitude_l = latitude_l
        self.longitude_l = longitude_l
        self.altitude_l = altitude_l
        self.config_topic = config_topic
        self.ads_b_input_topic = ads_b_input_topic
        self.ais_input_topic = ais_input_topic
        self.ledger_output_topic = ledger_output_topic
        self.selection_output_topic = selection_output_topic
        self.max_age = max_age
        self.max_aircraft_track_interval = max_aircraft_track_interval
        self.max_ship_track_interval = max_ship_track_interval
        self.drop_interval = drop_interval
        self.select_interval = select_interval
        self.heartbeat_interval = heartbeat_interval
        self.loop_sleep = loop_sleep

        # Compute position of the object ledger device in the geocentric
        # (XYZ) coordinate system
        self.r_XYZ_l = axis_ptz_utilities.compute_r_XYZ(
            self.longitude_l, self.latitude_l, self.altitude_l
        )

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
        self.computed_columns = [
            "distance",
            "timestamp_selected",
        ]
        self.ledger = pd.DataFrame(
            columns=self.required_columns + self.computed_columns
        )
        self.ledger.set_index("object_id", inplace=True)
        self.selected_object = None
        self.max_track_interval = {
            "aircraft": self.max_aircraft_track_interval * 60,
            "ship": self.max_ship_track_interval * 60,
        }

        # Log configuration parameters
        logging.info(
            f"""ObjectLedger initialized with parameters:
    hostname = {hostname}
    latitude_l = {latitude_l}
    longitude_l = {longitude_l}
    altitude_l = {altitude_l}
    config_topic = {config_topic}
    ads_b_input_topic = {ads_b_input_topic}
    ais_input_topic = {ais_input_topic}
    ledger_output_topic = {ledger_output_topic}
    selection_output_topic = {selection_output_topic}
    max_age = {max_age}
    max_aircraft_track_interval = {max_aircraft_track_interval}
    max_ship_track_interval = {max_ship_track_interval}
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
        # ignoring config message data without a "object-ledger" key
        if type(msg) == mqtt.MQTTMessage:
            data = self.decode_payload(msg.payload)
        else:
            data = msg["data"]
        if "object-ledger" not in data:
            return
        logging.info(f"Processing config message data: {data}")
        object_ledger = data["object-ledger"]
        self.latitude_l = object_ledger.get("latitude_l", self.latitude_l)
        self.longitude_l = object_ledger.get("longitude_l", self.longitude_l)
        self.altitude_l = object_ledger.get("altitude_l", self.altitude_l)
        self.config_topic = object_ledger.get("config_topic", self.config_topic)
        self.ads_b_input_topic = object_ledger.get(
            "ads_b_input_topic", self.ads_b_input_topic
        )
        self.ais_input_topic = object_ledger.get(
            "ais_input_topic", self.ais_input_topic
        )
        self.ledger_output_topic = object_ledger.get(
            "ledger_output_topic", self.ledger_output_topic
        )
        self.selection_output_topic = object_ledger.get(
            "selection_output_topic", self.selection_output_topic
        )
        self.max_age = object_ledger.get("max_age", self.max_age)
        self.max_aircraft_track_interval = object_ledger.get(
            "max_aircraft_track_interval", self.max_aircraft_track_interval
        )
        self.max_ship_track_interval = object_ledger.get(
            "max_ship_track_interval", self.max_ship_track_interval
        )
        self.drop_interval = object_ledger.get("drop_interval", self.drop_interval)
        self.select_interval = object_ledger.get(
            "select_interval", self.select_interval
        )
        self.heartbeat_interval = object_ledger.get(
            "heartbeat_interval", self.heartbeat_interval
        )
        self.loop_sleep = object_ledger.get("loop_sleep", self.loop_sleep)
        self.max_track_interval = {
            "aircraft": self.max_aircraft_track_interval * 60,
            "ship": self.max_ship_track_interval * 60,
        }

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

            # Initialize computed columns
            state["distance"] = 0.0
            state["timestamp_selected"] = 2 * datetime.utcnow().timestamp()

        except Exception as e:
            logging.error(f"Could not populate required state: {e}")

        # Process required state
        entry = pd.DataFrame(state, index=[state["object_id"]])
        entry.set_index("object_id", inplace=True)
        if entry.notna().all(axis=1).bool():
            # Compute position in the geocentric (XYZ) coordinate
            # system of the object
            r_XYZ_u = axis_ptz_utilities.compute_r_XYZ(
                entry["longitude"].to_list()[0],
                entry["latitude"].to_list()[0],
                entry["altitude"].to_list()[0],
            )

            # Compute distance between the object and the object ledger
            # device
            entry["distance"] = axis_ptz_utilities.norm(r_XYZ_u - self.r_XYZ_l)

            # Add or update the entry in the ledger
            if not entry.index.isin(self.ledger.index):
                logging.info(f"Adding entry state data for object id: {entry.index}")
                self.ledger = pd.concat([self.ledger, entry], ignore_index=False)

            else:
                logging.info(f"Updating entry state data for object id: {entry.index}")
                self.ledger.update(
                    entry.drop(["distance", "timestamp_selected"], axis=1)
                )

            # Send the full ledger to MQTT
            self._send_data(
                {
                    "type": "Full Ledger",
                    "payload": self.ledger.to_json(),
                }
            )

        else:
            logging.info(f"Invalid entry: {entry}")

    def _drop_entries(self: Any) -> None:
        """Drop ledger entries if their age exceeds the maximum."""
        index = self.ledger[
            (datetime.utcnow().timestamp() - self.ledger["timestamp"]) / 60
            > self.max_age
        ].index
        logging.info(f"Dropping entry for object ids: {index}")
        self.ledger.drop(
            index,
            inplace=True,
        )

    def get_max_track_interval(self, object_type: str) -> float:
        """Get the maximum track interval based on object type.

        Args:
            object_type (str): the object type: 'aircraft' or 'ship'

        Returns:
            (float): the maximum track interval [minutes]
        """
        return self.max_track_interval[object_type]

    def _select_object(self: Any) -> None:
        """Select the object that is closest to the object ledger
        device for tracking, provided either no object has been
        selected, or the currently selected object has been tracked at
        least as long as the maximum track interval.
        """
        # Keep the ledger sorted by distance
        if self.ledger.empty:
            logging.info("Ledger is empty: no object to select")
            return
        self.ledger.sort_values("distance", inplace=True)

        # Find objects that have not been tracked longer than the
        # maximum track interval
        selectable_objects = self.ledger[
            datetime.utcnow().timestamp() - self.ledger["timestamp_selected"]
            < self.ledger["object_type"].apply(self.get_max_track_interval)
        ]
        if selectable_objects.empty:
            # Any previously selected object has been tracked longer
            # than the maximum track interval
            logging.info("No selectable objects")
            self.selected_object = None
            return

        # Select the closest object if no object was previously
        # selected, or the previously selected object has been tracked
        # longer than the maximum track interval
        if (
            self.selected_object is None
            or self.selected_object.name not in selectable_objects.index
        ):
            timestamp_selected = datetime.utcnow().timestamp()
            self.selected_object = selectable_objects.iloc[0].copy()
            self.selected_object["object_id"] = self.selected_object.name
            self.selected_object.at["timestamp_selected"] = timestamp_selected
            self.ledger.loc[
                self.selected_object.name, "timestamp_selected"
            ] = timestamp_selected
            logging.info(f"Selected object with id: {self.selected_object.name}")

        # Send the selected object to MQTT
        self._send_data(
            {
                "type": "Selected Object",
                "payload": self.selected_object.to_json(),
            }
        )

    def _send_data(self: Any, data: Dict[str, str]) -> bool:
        """Leverages edgetech-core functionality to publish a JSON
        payload to the MQTT broker on the topic specified in the class
        constructor.

        Args:
            data (Dict[str, str]): Dictionary payload that maps keys
                to payload.

        Returns:
            bool: Returns True if successful publish else False
        """
        # TODO: Provide fields via environment or command line
        out_json = self.generate_payload_json(
            push_timestamp=str(int(datetime.utcnow().timestamp())),
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

        # Publish the data as JSON to the topic by type
        if data["type"] == "Full Ledger":
            send_data_topic = self.ledger_output_topic

        elif data["type"] == "Selected Object":
            send_data_topic = self.selection_output_topic

        else:
            logging.info(f"Unexpected data type: {data['type']}")

        success = self.publish_to_topic(send_data_topic, out_json)
        if success:
            logging.info(f"Successfully sent data on channel {send_data_topic}: {data}")
        else:
            logging.info(f"Failed to send data on channel {send_data_topic}: {data}")

        # Return True if successful else False
        return success

    def main(self: Any) -> None:
        """Schedule methods, subscribe to required topics, and loop."""

        # Schedule module heartbeat
        schedule.every(self.heartbeat_interval).seconds.do(
            self.publish_heartbeat, payload="Object Ledger Module Heartbeat"
        )

        # Schedule dropping of ledger entries after their age exceeds
        # the maximum
        schedule.every(self.drop_interval).seconds.do(self._drop_entries)

        # Schedule selection of a object in the ledger for tracking
        schedule.every(self.select_interval).seconds.do(self._select_object)

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

            # except Exception as exception:
            #     logging.error(f"Main loop exception: {exception}")


def make_ledger() -> ObjectLedger:
    return ObjectLedger(
        mqtt_ip=os.getenv("MQTT_IP", "mqtt"),
        hostname=os.environ.get("HOSTNAME", ""),
        latitude_l=float(os.getenv("LATITUDE_L", 0.0)),
        longitude_l=float(os.getenv("LONGITUDE_L", 0.0)),
        altitude_l=float(os.getenv("ALTITUDE_L", 0.0)),
        config_topic=os.getenv("CONFIG_TOPIC", "TBC"),
        ads_b_input_topic=os.getenv("ADS_B_INPUT_TOPIC", "TBC"),
        ais_input_topic=os.getenv("AIS_INPUT_TOPIC", "TBC"),
        ledger_output_topic=os.getenv("LEDGER_OUTPUT_TOPIC", "TBC"),
        selection_output_topic=os.getenv("SELECTION_OUTPUT_TOPIC", "TBC"),
        max_age=float(os.getenv("MAX_AGE", 10.0)),
        max_aircraft_track_interval=float(
            os.getenv("MAX_AIRCRAFT_TRACK_INTERVAL", 4.0)
        ),
        max_ship_track_interval=float(os.getenv("MAX_SHIP_TRACK_INTERVAL", 1.0)),
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
