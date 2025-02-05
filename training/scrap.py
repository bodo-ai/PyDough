"""
Sandbox file for testing LLM training/interactions with the sample PyDough data.
"""

import json
import os

import pandas as pd


def get_graphs() -> dict[str, dict]:
    """
    Returns a mapping of each graph name within any graph file in the graphs
    directory to the raw JSON metadata for that graph.
    """
    result: dict[str, dict] = {}

    # Loop over every json file in the graphs folder
    for file_name in os.listdir(f"{os.path.dirname(__file__)}/graphs"):
        if file_name.endswith(".json"):
            # Load the JSON, then dump every top-level key-value pair into
            # the result.
            fpath: str = f"{os.path.dirname(__file__)}/graphs/{file_name}"
            with open(fpath) as f:
                result.update(json.load(f))

    return result


def run(training_data: pd.DataFrame, graphs_json: dict[str, dict]):
    """
    TODO: implement logic using the training data & the available graphs
    """
    pass


if __name__ == "__main__":
    training_data: pd.DataFrame = pd.read_csv(
        f"{os.path.dirname(__file__)}/pydough_corpus.csv"
    )
    graphs_json: dict[str, dict] = get_graphs()
    run(training_data, graphs_json)
