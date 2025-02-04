"""
TODO
"""

import json
import os

import pandas as pd

from pydough import parse_json_metadata_from_file
from pydough.metadata import GraphMetadata


def get_graphs() -> dict[tuple[str, str], GraphMetadata]:
    """
    Returns a mapping of each graph file name & graph within those files to the
    metadata for that graph.
    """
    # First identify every such combination
    combinations: list[tuple[str, str]] = []
    for file_name in os.listdir("graphs"):
        with open(f"graphs/{file_name}") as f:
            json_graph: dict = json.load(f)
        graph_names = json_graph.keys()
        combinations.extend([(file_name, graph_name) for graph_name in graph_names])

    # Then parse all of the combinations
    return {
        (file_name, graph_name): parse_json_metadata_from_file(file_name, graph_name)
        for file_name, graph_name in combinations
    }


def run():
    pd.read_csv("pydough_corpus.tsv", quotechar="`")
    breakpoint()
    pass


if __name__ == "__main__":
    run()
