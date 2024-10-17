"""
TODO: add file-level docstring.
"""

import pydough
import pytest
from pydough.metadata.graphs import GraphMetadata


@pytest.fixture(scope="session")
def amazon_graph_path():
    """
    Tuple of the path to the JSON file containing the Amazon graph as well as
    a the name that should be used to fetch the Amazon graph.
    """
    return "tests/test_metadata/sample_graphs.json", "Amazon"


@pytest.fixture(scope="session")
def tpch_graph_path():
    """
    Tuple of the path to the JSON file containing the TPCH graph as well as
    a the name that should be used to fetch the TPCH graph.
    """
    return "tests/test_metadata/sample_graphs.json", "TPCH"


@pytest.fixture(scope="session")
def empty_graph_path():
    """
    Tuple of the path to the JSON file containing the Empty graph as well as
    a the name that should be used to fetch the Empty graph.
    """
    return "tests/test_metadata/sample_graphs.json", "empty"


@pytest.fixture
def get_graph(amazon_graph_path, tpch_graph_path, empty_graph_path):
    """
    A function that takes in the name of a graph (currently only supports the
    values 'amazon', 'tpch', and 'empty') and returns the metadata for that
    PyDough graph.
    """

    def impl(name: str) -> GraphMetadata:
        if name == "amazon":
            file_path, graph_name = amazon_graph_path
        elif name == "tpch":
            file_path, graph_name = tpch_graph_path
        elif name == "empty":
            file_path, graph_name = empty_graph_path
        else:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=file_path, graph_name=graph_name
        )

    return impl


@pytest.fixture(params=["amazon", "tpch", "empty"])
def sample_graphs(request, get_graph):
    """
    Retrieves the PyDough metadata for each graph in the `sample_graphs` JSON
    file.
    """
    return get_graph(request.param)
