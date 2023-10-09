import json


def test_integration() -> None:
    """
    Test integration of the object-ledger from test.sh.
    """
    with open("./test-data/test-output.json") as output_file:
        output = json.load(output_file)

    # TODO: Implement test
