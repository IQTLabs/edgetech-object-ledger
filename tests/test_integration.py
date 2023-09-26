import json
import pandas as pd
from matplotlib import pyplot as plt


def test_integration() -> None:
    """
    Tests integration of the message handler with the MQTT client. Designed to be run after docker-compose.test.yaml.
    Recommended to run from test.sh.
    """

    with open("./test-data/test-output.json") as output_file:
        output = json.load(output_file)

    # TODO: Implement test
