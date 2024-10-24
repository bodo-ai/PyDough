"""
TODO: add file-level docstring.
"""

import pydough
import pytest
import json
from pydough.metadata.graphs import GraphMetadata
from typing import Dict, Set

from test_utils import graph_fetcher, noun_fetcher


@pytest.fixture(scope="session")
def sample_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the sample graphs.
    """
    return "tests/test_metadata/sample_graphs.json"


@pytest.fixture(scope="session")
def invalid_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the invalid graphs.
    """
    return "tests/test_metadata/invalid_graphs.json"


@pytest.fixture(scope="session")
def sample_graph_nouns_path() -> str:
    """
    Tuple of the path to the JSON file containing the nouns for each
    of the sample graphs.
    """
    return "tests/test_metadata/sample_graphs_nouns.json"


@pytest.fixture(scope="session")
def amazon_graph_name() -> str:
    """
    The name of the field in the sample graph JSON file corresponding
    to the AMAZON graph.
    """
    return "Amazon"


@pytest.fixture(scope="session")
def tpch_graph_name() -> str:
    """
    The name of the field in the sample graph JSON file corresponding
    to the TPCH graph.
    """
    return "TPCH"


@pytest.fixture(scope="session")
def empty_graph_name() -> str:
    """
    The name of the field in the sample graph JSON file corresponding
    to the empty graph.
    """
    return "Empty"


@pytest.fixture
def get_sample_graph(
    sample_graph_path: str,
    amazon_graph_name: str,
    tpch_graph_name: str,
    empty_graph_name: str,
) -> graph_fetcher:
    """
    A function that takes in the name of a graph (currently only supports the
    values 'amazon', 'tpch', and 'empty') and returns the metadata for that
    PyDough graph.
    """

    def impl(name: str) -> GraphMetadata:
        if name == "amazon":
            graph_name = amazon_graph_name
        elif name == "tpch":
            graph_name = tpch_graph_name
        elif name == "empty":
            graph_name = empty_graph_name
        else:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=sample_graph_path, graph_name=graph_name
        )

    return impl


@pytest.fixture(params=["amazon", "tpch", "empty"])
def sample_graph_names(request) -> str:
    """
    Fixture for the names that each of the sample graphs can be accessed by.
    """
    return request.param


@pytest.fixture
def get_sample_graph_nouns(
    sample_graph_nouns_path, amazon_graph_name, tpch_graph_name, empty_graph_name
) -> noun_fetcher:
    """
    A function that takes in the name of a graph (currently only supports the
    values 'amazon', 'tpch', and 'empty') and returns the metadata for that
    PyDough graph.
    """

    def impl(name: str) -> GraphMetadata:
        if name == "amazon":
            graph_name = amazon_graph_name
        elif name == "tpch":
            graph_name = tpch_graph_name
        elif name == "empty":
            graph_name = empty_graph_name
        else:
            raise Exception(f"Unrecognized graph name '{name}'")
        nouns: Dict[str, Set[str]]
        with open(sample_graph_nouns_path, "r") as f:
            nouns = json.load(f)[graph_name]
        return nouns

    return impl


@pytest.fixture
def sample_graphs(
    sample_graph_names: str, get_sample_graph: graph_fetcher
) -> GraphMetadata:
    """
    Retrieves the PyDough metadata for each graph in the `sample_graphs` JSON
    file.
    """
    return get_sample_graph(sample_graph_names)
