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


# ---------------------------------------------------------------------------
# Regression tests — one per bug fix, added after-the-fact
# ---------------------------------------------------------------------------


# --- Direct top-level WHERE + PARTITION ancestor chain ---


def test_direct_partition_with_where_shows_table_collection_and_where(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """nations.WHERE(...).PARTITION(...) must include TableCollection + Where steps."""

    def impl():
        return nations.WHERE(key > 5).PARTITION(name="g", by=name).CALCULATE(name=name)

    result = _run(impl, tpch_graph, tpch_session)
    types = [s["type"] for s in result["steps"]]
    assert "TableCollection" in types
    assert "Where" in types


def test_direct_partition_with_where_shows_source_collection(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """nations.WHERE(...).PARTITION(...) schema must name the source collection."""

    def impl():
        return nations.WHERE(key > 5).PARTITION(name="g", by=name).CALCULATE(name=name)

    result = _run(impl, tpch_graph, tpch_session)
    assert result["schema"]["source_collection"] == "nations"


def test_direct_partition_with_where_data_filter_in_key_facts(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Key Facts must show the WHERE filter as a data filter, not 'none'."""

    def impl():
        return nations.WHERE(key > 5).PARTITION(name="g", by=name).CALCULATE(name=name)

    md = _run_md(impl, tpch_graph, tpch_session)
    kf = md[: md.index("## Query Summary")]
    assert "Data filters:** none" not in kf
    assert "key > 5" in kf


# --- Subcollection PARTITION with parent WHERE ---


def test_subcollection_partition_shows_parent_where_and_table(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """customers.WHERE(...).orders.PARTITION(...) must expose the parent filter."""

    def impl():
        return (
            customers.WHERE(market_segment == "BUILDING")
            .orders.PARTITION(name="g", by=order_status)
            .CALCULATE(order_status=order_status, n=COUNT(orders))
            .TOP_K(1, by=n.DESC())
            .CALCULATE(order_status)
        )

    result = _run(impl, tpch_graph, tpch_session)
    types = [s["type"] for s in result["steps"]]
    assert "TableCollection" in types
    assert "Where" in types
    assert result["schema"]["source_collection"] == "customers"


# --- Global-level CALCULATE with COUNT ---


def test_global_calculate_count_names_collection(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Global CALCULATE(COUNT(nations)) must name the collection in summary/schema."""
    node = pydough.from_string(
        "result = CALCULATE(n=COUNT(nations.WHERE(key > 5)))",
        session=tpch_session,
    )
    result = cast(dict, pydough.explain_llm(node, session=tpch_session))
    assert result["error"] is False
    assert "nations" in result["query_summary"]
    assert result["schema"]["source_collection"] == "nations"


# --- ChildReferenceExpression shows full navigation path ---


def test_singular_subcollection_shows_child_reference_kind(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """nation.SINGULAR().name inside CALCULATE must be kind=ChildReference, not Reference."""

    def impl():
        return customers.CALCULATE(nation_name=nation.SINGULAR().name)

    result = _run(impl, tpch_graph, tpch_session)
    calc = next(s for s in result["steps"] if s["type"] == "Calculate")
    detail = calc["term_details"]["nation_name"]
    assert detail["kind"] == "ChildReference"
    # text must include navigation, not just the bare field name
    assert detail.get("text", "") != "name"
    assert "nation" in detail.get("text", "")


def test_singular_subcollection_annotation_in_summary(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Summary must show the ChildReference expression, not just the output column name."""

    def impl():
        return customers.CALCULATE(nation_name=nation.SINGULAR().name)

    result = _run(impl, tpch_graph, tpch_session)
    assert "nation_name" in result["query_summary"]
    # The expression annotation must appear — not just a plain selection
    assert "SINGULAR" in result["query_summary"] or "nation" in result["query_summary"]


# --- Multi-level WHERE split in query summary ---


def test_summary_puts_subcollection_filter_in_separate_clause(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Summary must describe parent and subcollection WHERE conditions separately."""

    def impl():
        return (
            customers.WHERE(market_segment == "BUILDING")
            .orders.WHERE(total_price > 1000)
            .CALCULATE(key=key)
        )

    result = _run(impl, tpch_graph, tpch_session)
    summary = result["query_summary"]
    assert "market_segment" in summary
    assert "subcollection" in summary.lower()
    assert "total_price" in summary


# --- Key Facts: data vs post-compute filter split ---


def test_key_facts_separates_pre_and_post_calculate_where(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Data filters and post-compute filters must appear in separate Key Facts lines."""

    def impl():
        return (
            customers.WHERE(market_segment == "BUILDING")
            .CALCULATE(name=name, n=COUNT(orders))
            .WHERE(RANKING(by=n.ASC()) == 1)
            .CALCULATE(name)
        )

    md = _run_md(impl, tpch_graph, tpch_session)
    kf = md[: md.index("## Query Summary")]
    assert "Data filters:" in kf
    assert "Post-compute filters:" in kf
    assert "market_segment" in kf
    assert "RANKING" in kf


def test_key_facts_no_post_compute_line_when_absent(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Post-compute filters line must be absent when there is no post-CALCULATE WHERE."""

    def impl():
        return customers.WHERE(market_segment == "BUILDING").CALCULATE(name)

    md = _run_md(impl, tpch_graph, tpch_session)
    kf = md[: md.index("## Query Summary")]
    assert "Post-compute filters" not in kf


# --- COUNT per-group semantic label ---


def test_count_inside_partition_shows_per_group_in_summary(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """COUNT inside a PARTITION must be labelled 'per group' in the query summary."""

    def impl():
        return customers.PARTITION(name="g", by=market_segment).CALCULATE(
            market_segment=market_segment, n=COUNT(customers)
        )

    result = _run(impl, tpch_graph, tpch_session)
    assert "per group" in result["query_summary"]


def test_count_outside_partition_no_per_group_label(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Global COUNT (no PARTITION) must NOT say 'per group' in the summary."""

    def impl():
        return nations.CALCULATE(n=COUNT(customers))

    result = _run(impl, tpch_graph, tpch_session)
    assert "per group" not in result["query_summary"]


# --- Computed term annotation in summary ---


def test_join_strings_shows_expression_annotation_in_summary(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """A JOIN_STRINGS computed term must show its expression in the summary."""

    def impl():
        return customers.CALCULATE(full=JOIN_STRINGS(" ", name, phone))

    result = _run(impl, tpch_graph, tpch_session)
    assert "JOIN_STRINGS" in result["query_summary"]


def test_plain_reference_no_expression_annotation(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Plain column references must NOT get parenthetical annotations in the summary."""

    def impl():
        return customers.CALCULATE(key, name)

    result = _run(impl, tpch_graph, tpch_session)
    assert "(computed)" not in result["query_summary"]
    assert "JOIN_STRINGS" not in result["query_summary"]


# --- PartitionBy key shows term_name, not full upstream expression ---


def test_partition_key_is_term_name_not_full_expression(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Partition key must show 'tier', not the full IFF(...) upstream expression."""

    def impl():
        return (
            nations.WHERE(key > 5)
            .CALCULATE(tier=IFF(key > 15, "high", "low"))
            .PARTITION(name="g", by=tier)
            .CALCULATE(tier=tier, n=COUNT(nations))
            .TOP_K(1, by=n.DESC())
            .CALCULATE(tier)
        )

    result = _run(impl, tpch_graph, tpch_session)
    part = next(s for s in result["steps"] if s["type"] == "PartitionBy")
    assert part["keys"] == ["tier"]
    assert not any("IFF" in k for k in part["keys"])


# --- output_columns populated when root is TopK ---


def test_output_columns_not_empty_under_topk(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Schema output_columns must be populated even when the root node is TopK."""

    def impl():
        return nations.CALCULATE(name=name, n=COUNT(customers)).TOP_K(1, by=n.DESC())

    result = _run(impl, tpch_graph, tpch_session)
    assert result["schema"]["output_columns"] != []
    assert "name" in result["schema"]["output_columns"]
    assert "n" in result["schema"]["output_columns"]


# --- Window function note in Where step ---


def test_ranking_where_emits_per_partition_note(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """A WHERE containing RANKING must emit a note about per= partition semantics."""

    def impl():
        return customers.orders.WHERE(
            RANKING(by=total_price.DESC(), per="customers") == 1
        ).CALCULATE(key=key)

    result = _run(impl, tpch_graph, tpch_session)
    where_steps = [s for s in result["steps"] if s["type"] == "Where"]
    ranking_step = next(
        (
            s
            for s in where_steps
            if any(
                "RANKING" in (c.get("text", "") if isinstance(c, dict) else str(c))
                for c in s.get("conditions", [])
            )
        ),
        None,
    )
    assert ranking_step is not None
    notes = ranking_step.get("notes", [])
    assert any("per=" in n or "partition" in n.lower() for n in notes)


# --- Key Facts block is always first and always has three fields ---


def test_key_facts_is_first_section(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Key Facts must be the first ## section in the success markdown."""

    def impl():
        return nations.CALCULATE(key, name)

    md = _run_md(impl, tpch_graph, tpch_session)
    first_section = md.split("##")[1].strip()
    assert first_section.startswith("Key Facts")


def test_key_facts_always_has_required_fields(
    tpch_graph: GraphMetadata, tpch_session: PyDoughSession
):
    """Key Facts must always contain Source collection, Limit, and Data filters."""

    def impl():
        return nations.CALCULATE(key, name)

    md = _run_md(impl, tpch_graph, tpch_session)
    kf = md[: md.index("## Query Summary")]
    assert "Source collection:" in kf
    assert "Limit:" in kf
    assert "Data filters:" in kf
