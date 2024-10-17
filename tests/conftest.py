"""
TODO: add file-level docstring.
"""

import pydough
import pytest


@pytest.fixture(scope="session")
def amazon_graph_path():
    """
    TODO: add docstring describing this fixture
    """
    return "tests/test_metadata/sample_graphs.json", "Amazon"


@pytest.fixture(scope="session")
def tpch_graph_path():
    """
    TODO: add docstring describing this fixture
    """
    return "tests/test_metadata/sample_graphs.json", "TPCH"


@pytest.fixture(scope="session")
def empty_graph_path():
    """
    TODO: add docstring describing this fixture
    """
    return "tests/test_metadata/sample_graphs.json", "empty"


@pytest.fixture
def amazon_graph(amazon_graph_path):
    """
    TODO: add docstring describing this fixture
    """
    file_path, graph_name = amazon_graph_path
    return pydough.parse_json_metadata_from_file(
        file_path=file_path, graph_name=graph_name
    )


@pytest.fixture
def tpch_graph(tpch_graph_path):
    """
    TODO: add docstring describing this fixture
    """
    file_path, graph_name = tpch_graph_path
    return pydough.parse_json_metadata_from_file(
        file_path=file_path, graph_name=graph_name
    )


@pytest.fixture
def empty_graph(empty_graph_path):
    """
    TODO: add docstring describing this fixture
    """
    file_path, graph_name = empty_graph_path
    return pydough.parse_json_metadata_from_file(
        file_path=file_path, graph_name=graph_name
    )


@pytest.fixture
def get_graph(amazon_graph, tpch_graph, empty_graph):
    """
    TODO: add docstring describing this fixture
    """

    def impl(graph_name):
        if graph_name == "amazon":
            return amazon_graph
        elif graph_name == "tpch":
            return tpch_graph
        elif graph_name == "empty":
            return empty_graph
        else:
            raise Exception(f"Unrecognized graph name '{graph_name}'")

    return impl


@pytest.fixture(params=["amazon", "tpch", "empty"])
def sample_graphs(request, get_graph):
    return get_graph(request.param)
