"""
Tests for ``pydough.explain_llm``.

Each test verifies the structure and key fields of the JSON output without
pinning every string, so the tests remain stable across minor wording changes.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

from collections.abc import Callable
from typing import cast

import pytest

import pydough
from pydough.configs import PyDoughSession
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from tests.testing_utilities import graph_fetcher

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(
    impl: Callable[[], UnqualifiedNode],
    graph: GraphMetadata,
    session: PyDoughSession,
) -> dict:
    """Qualify ``impl`` under ``graph`` and call ``explain_llm``."""
    node: UnqualifiedNode = pydough.init_pydough_context(graph)(impl)()
    return cast(dict, pydough.explain_llm(node, session=session))


def _step(result: dict, order: int) -> dict:
    """Return the step with the given 1-based order."""
    return next(s for s in result["steps"] if s["order"] == order)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tpch_session(get_sample_graph: graph_fetcher) -> PyDoughSession:
    """A PyDoughSession loaded with the TPCH graph (no DB connection needed)."""
    graph: GraphMetadata = get_sample_graph("TPCH")
    session = PyDoughSession()
    session.metadata = graph
    return session


@pytest.fixture
def tpch_graph(get_sample_graph: graph_fetcher) -> GraphMetadata:
    return get_sample_graph("TPCH")


# ---------------------------------------------------------------------------
# Top-level output shape
# ---------------------------------------------------------------------------


def test_success_shape(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """A valid expression returns the full success shape."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)

    assert result["error"] is False
    assert isinstance(result["query_summary"], str) and result["query_summary"]
    assert isinstance(result["steps"], list) and len(result["steps"]) > 0
    schema = result["schema"]
    assert isinstance(schema, dict)
    assert "source_collection" in schema
    assert "output_columns" in schema
    assert "column_types" in schema
    assert "ordering" in schema
    assert "limit" in schema


def test_error_shape(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """An unrecognised term returns the error payload with consistent shape."""

    def impl():
        return nations.WHERE(typo_field == "ASIA")

    result = _run(impl, tpch_graph, tpch_session)

    assert result["error"] is True
    assert isinstance(result["message"], str) and result["message"]
    assert result["steps"] == []
    assert result["schema"] is None


# ---------------------------------------------------------------------------
# Step ordering and types
# ---------------------------------------------------------------------------


def test_step_order_simple(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """
    nations.WHERE(region.name == 'ASIA').CALCULATE(key, name) →
    GlobalContext → TableCollection → Where → Calculate
    """

    def impl():
        return nations.WHERE(region.name == "ASIA").CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    types = [s["type"] for s in result["steps"]]

    assert types[0] == "GlobalContext"
    assert "TableCollection" in types
    assert "Where" in types
    assert types[-1] == "Calculate"
    assert types.index("GlobalContext") < types.index("TableCollection")
    assert types.index("TableCollection") < types.index("Where")
    assert types.index("Where") < types.index("Calculate")


def test_every_step_has_notes_key(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Every step must emit a 'notes' list, even when empty."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    for step in result["steps"]:
        assert "notes" in step
        assert isinstance(step["notes"], list)


def test_every_step_has_debug_available_terms(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Every step must emit 'debug.available_terms' with 'expressions' and
    'collections'."""

    def impl():
        return nations.WHERE(region.name == "ASIA").CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    for step in result["steps"]:
        at = step["debug"]["available_terms"]
        assert isinstance(at["expressions"], list)
        assert isinstance(at["collections"], list)


# ---------------------------------------------------------------------------
# GlobalContext step
# ---------------------------------------------------------------------------


def test_global_context_step(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """GlobalContext step exposes top-level collections, no expressions."""

    def impl():
        return nations.CALCULATE(key)

    result = _run(impl, tpch_graph, tpch_session)
    gc = _step(result, 1)

    assert gc["type"] == "GlobalContext"
    assert "nations" in gc["debug"]["available_terms"]["collections"]
    assert gc["debug"]["available_terms"]["expressions"] == []


# ---------------------------------------------------------------------------
# TableCollection step
# ---------------------------------------------------------------------------


def test_table_collection_step(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """TableCollection step records the collection name and its scalar terms."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    tc = next(s for s in result["steps"] if s["type"] == "TableCollection")

    assert tc["collection"] == "nations"
    assert "key" in tc["debug"]["available_terms"]["expressions"]
    assert "name" in tc["debug"]["available_terms"]["expressions"]


# ---------------------------------------------------------------------------
# Where step
# ---------------------------------------------------------------------------


def test_where_step_single_condition(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Single-condition Where step: conditions list has one dict entry."""

    def impl():
        return nations.WHERE(key > 5)

    result = _run(impl, tpch_graph, tpch_session)
    where = next(s for s in result["steps"] if s["type"] == "Where")

    assert len(where["conditions"]) == 1
    # conditions are now dicts; condition_summary is still a plain string
    assert isinstance(where["conditions"][0], dict)
    assert isinstance(where["condition_summary"], str)
    # Where no longer emits a generic retention note
    assert where["notes"] == []


def test_where_step_and_conditions_split(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """AND conditions are split into separate dict entries in conditions list."""

    def impl():
        return nations.WHERE((key > 5) & (key < 20))

    result = _run(impl, tpch_graph, tpch_session)
    where = next(s for s in result["steps"] if s["type"] == "Where")

    assert len(where["conditions"]) == 2
    assert all(isinstance(c, dict) for c in where["conditions"])
    assert "&" in where["condition_summary"]


def test_where_condition_structured(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Each condition dict has kind, operator, left, and right for BinaryOp."""

    def impl():
        return nations.WHERE(key > 5)

    result = _run(impl, tpch_graph, tpch_session)
    where = next(s for s in result["steps"] if s["type"] == "Where")
    cond = where["conditions"][0]

    assert cond["kind"] == "BinaryOp"
    assert "operator" in cond
    assert "left" in cond
    assert "right" in cond
    # left should be a Reference to 'key'
    assert cond["left"]["kind"] == "Reference"
    assert cond["left"]["term_name"] == "key"
    # right should be a Literal with value 5
    assert cond["right"]["kind"] == "Literal"
    assert cond["right"]["value"] == 5


# ---------------------------------------------------------------------------
# Calculate step
# ---------------------------------------------------------------------------


def test_calculate_term_details(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Calculate step records term_details for each computed expression."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    calc = next(s for s in result["steps"] if s["type"] == "Calculate")

    assert set(calc["terms"]) == {"key", "name"}
    assert calc["term_details"]["key"]["kind"] == "Reference"
    assert calc["term_details"]["name"]["kind"] == "Reference"
    # Reference no longer has a redundant 'text' field
    assert "text" not in calc["term_details"]["key"]
    assert "text" not in calc["term_details"]["name"]


def test_calculate_aggregation_implicit_scope_note(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """
    COUNT(orders) inside customers.CALCULATE → implicit_scope_note set,
    informational note emitted (not a warning).
    """

    def impl():
        return customers.CALCULATE(key, n_orders=COUNT(orders))

    result = _run(impl, tpch_graph, tpch_session)
    calc = next(s for s in result["steps"] if s["type"] == "Calculate")

    n_orders = calc["term_details"]["n_orders"]
    assert n_orders["kind"] == "Aggregation"
    assert n_orders["function"] == "COUNT"
    arg = n_orders["args"][0]
    assert arg["name"] == "orders"
    assert arg["access_path"] == ["orders"]
    assert arg["implicit_scope_note"] is not None

    notes = calc["notes"]
    assert any("implicit" in n.lower() or "relationship" in n.lower() for n in notes)
    assert not any("warning" in n.lower() for n in notes)


# ---------------------------------------------------------------------------
# CROSS step
# ---------------------------------------------------------------------------


def test_cross_step_type_and_note(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """nations.CROSS(regions) produces a 'Cross' step with both names in note."""

    def impl():
        return nations.CROSS(regions)

    result = _run(impl, tpch_graph, tpch_session)
    cross = next(s for s in result["steps"] if s["type"] == "Cross")

    assert cross["left"] == "nations"
    assert cross["right"] == "regions"
    assert len(cross["notes"]) == 1
    assert "nations" in cross["notes"][0]
    assert "regions" in cross["notes"][0]


# ---------------------------------------------------------------------------
# OrderBy / TopK step
# ---------------------------------------------------------------------------


def test_order_by_step(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """ORDER_BY produces an OrderBy step with collation details."""

    def impl():
        return nations.ORDER_BY(name.ASC())

    result = _run(impl, tpch_graph, tpch_session)
    ob = next(s for s in result["steps"] if s["type"] == "OrderBy")

    assert len(ob["collation"]) == 1
    assert ob["collation"][0]["direction"] == "ASC"


def test_topk_step(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """TOP_K produces a TopK step with a limit field."""

    def impl():
        return nations.TOP_K(5, by=name.ASC())

    result = _run(impl, tpch_graph, tpch_session)
    topk = next(s for s in result["steps"] if s["type"] == "TopK")

    assert topk["limit"] == 5
    assert topk["collation"][0]["direction"] == "ASC"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_schema_output_columns(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """Schema output_columns matches the CALCULATE terms."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    schema = result["schema"]

    assert set(schema["output_columns"]) == {"key", "name"}
    assert schema["source_collection"] == "nations"
    assert "available_collections" not in schema


def test_schema_column_types(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """Schema column_types includes an entry for every output column."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    schema = result["schema"]

    assert set(schema["column_types"].keys()) == set(schema["output_columns"])
    for col_type in schema["column_types"].values():
        assert isinstance(col_type, str) and col_type


def test_schema_no_output_columns_without_calculate(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """A collection with no explicit CALCULATE has an empty output_columns list."""

    def impl():
        return nations.WHERE(key > 5)

    result = _run(impl, tpch_graph, tpch_session)
    assert result["schema"]["output_columns"] == []


def test_schema_ordering_and_limit(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """TOP_K surfaces limit and ordering in schema for direct judge inspection."""

    def impl():
        return nations.TOP_K(5, by=name.ASC())

    result = _run(impl, tpch_graph, tpch_session)
    schema = result["schema"]

    assert schema["limit"] == 5
    assert len(schema["ordering"]) == 1
    assert schema["ordering"][0]["direction"] == "ASC"


def test_schema_no_ordering_without_sort(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """A collection with no ORDER_BY or TOP_K has empty ordering and null limit."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    schema = result["schema"]

    assert schema["ordering"] == []
    assert schema["limit"] is None


# ---------------------------------------------------------------------------
# query_summary
# ---------------------------------------------------------------------------


def test_query_summary_present(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """Success result always contains a non-empty query_summary string."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    assert isinstance(result["query_summary"], str)
    assert result["query_summary"]


def test_query_summary_contains_collection(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """query_summary names the source collection."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    assert "nations" in result["query_summary"]


def test_query_summary_contains_filter(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """query_summary mentions the filter condition when WHERE is present."""

    def impl():
        return nations.WHERE(key > 5).CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    assert "filtered" in result["query_summary"].lower()


def test_query_summary_contains_aggregation(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """query_summary describes COUNT aggregations in the compute clause."""

    def impl():
        return nations.CALCULATE(key, n_customers=COUNT(customers))

    result = _run(impl, tpch_graph, tpch_session)
    summary = result["query_summary"].lower()
    assert "count" in summary
    assert "n_customers" in summary


def test_query_summary_topk(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """query_summary mentions the limit when TOP_K is present."""

    def impl():
        return nations.TOP_K(5, by=name.ASC())

    result = _run(impl, tpch_graph, tpch_session)
    assert "5" in result["query_summary"]


# ---------------------------------------------------------------------------
# format="md"
# ---------------------------------------------------------------------------


def _run_md(
    impl: Callable[[], UnqualifiedNode],
    graph: GraphMetadata,
    session: PyDoughSession,
) -> str:
    """Qualify ``impl`` under ``graph`` and call ``explain_llm(format='md')``."""
    node: UnqualifiedNode = pydough.init_pydough_context(graph)(impl)()
    return pydough.explain_llm(node, session=session, format="md")


def test_md_success_returns_string(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """format='md' returns a str, not a dict."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run_md(impl, tpch_graph, tpch_session)
    assert isinstance(result, str)


def test_md_error_returns_string(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """format='md' error path also returns a str."""

    def impl():
        return nations.WHERE(typo_field == "ASIA")

    result = _run_md(impl, tpch_graph, tpch_session)
    assert isinstance(result, str)


def test_md_success_sections(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """Success markdown contains the three top-level sections."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run_md(impl, tpch_graph, tpch_session)
    assert "## Query Summary" in result
    assert "## Steps" in result
    assert "## Schema" in result


def test_md_error_section(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """Error markdown starts with ## Error and contains the message."""

    def impl():
        return nations.WHERE(typo_field == "ASIA")

    result = _run_md(impl, tpch_graph, tpch_session)
    assert result.startswith("## Error")
    # No Steps or Schema sections in an error payload
    assert "## Steps" not in result
    assert "## Schema" not in result


def test_md_contains_collection_name(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Collection name appears in the markdown output."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run_md(impl, tpch_graph, tpch_session)
    assert "nations" in result


def test_md_contains_query_summary(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """The query_summary sentence is embedded in the markdown."""

    def impl():
        return nations.CALCULATE(key, name)

    md = _run_md(impl, tpch_graph, tpch_session)
    json_result = _run(impl, tpch_graph, tpch_session)
    # The same deterministic summary must appear verbatim in the markdown.
    assert json_result["query_summary"] in md


def test_md_where_condition_visible(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """A WHERE condition is surfaced in the markdown steps."""

    def impl():
        return nations.WHERE(key > 5).CALCULATE(key, name)

    result = _run_md(impl, tpch_graph, tpch_session)
    assert "Condition" in result


def test_md_schema_output_columns(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Schema section lists the CALCULATE output columns."""

    def impl():
        return nations.CALCULATE(key, name)

    result = _run_md(impl, tpch_graph, tpch_session)
    assert "Output columns" in result
    assert "key" in result
    assert "name" in result


def test_md_topk_limit_visible(tpch_graph: GraphMetadata, tpch_session: PyDoughSession):
    """TOP_K limit appears in both the Steps and Schema sections."""

    def impl():
        return nations.TOP_K(5, by=name.ASC())

    result = _run_md(impl, tpch_graph, tpch_session)
    assert "5" in result
    assert "Limit" in result


def test_md_invalid_format_raises(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Passing an unrecognised format raises ValueError."""
    import pytest

    def impl():
        return nations.CALCULATE(key, name)

    node: UnqualifiedNode = pydough.init_pydough_context(tpch_graph)(impl)()
    with pytest.raises(ValueError, match="format"):
        pydough.explain_llm(node, session=tpch_session, format="xml")
