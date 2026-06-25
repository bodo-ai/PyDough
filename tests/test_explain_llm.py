"""
Tests for ``pydough.explain_llm``.

Reference-file tests (at the bottom of this file) contain the *full* JSON and
markdown output for representative scenarios.  Re-generate them by running:

    PYDOUGH_UPDATE_TESTS=1 uv run pytest tests/test_explain_llm.py -k refsol

The ``_classify_error`` unit tests and ``test_md_invalid_format_raises`` are
kept as targeted assertions because they test logic that does not produce an
``explain_llm`` output payload.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import json
from pathlib import Path
from typing import cast

import pytest

import pydough
from pydough.configs import PyDoughSession
from pydough.errors import (
    PyDoughMetadataException,
    PyDoughQDAGException,
    PyDoughSQLException,
    PyDoughSessionException,
    PyDoughTypeException,
    PyDoughUnqualifiedException,
)
from pydough.exploration.explain_llm import _classify_error
from pydough.unqualified import UnqualifiedNode


# ---------------------------------------------------------------------------
# _classify_error — full error taxonomy
# ---------------------------------------------------------------------------


def test_classify_syntax_error():
    """SyntaxError → 'syntax_error' with a non-None hint."""
    e = SyntaxError("invalid syntax")
    error_type, details, hint = _classify_error(e)
    assert error_type == "syntax_error"
    assert hint is not None


def test_classify_syntax_error_captures_location():
    """SyntaxError with line/offset info is captured in details."""
    e = SyntaxError("invalid syntax")
    e.lineno = 3
    e.offset = 7
    _, details, _ = _classify_error(e)
    assert details.get("line") == 3
    assert details.get("offset") == 7


def test_classify_answer_variable_error():
    """PyDoughUnqualifiedException mentioning 'answer' → 'answer_variable'."""
    e = PyDoughUnqualifiedException(
        "Expected variable 'result' to store answer, found None"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "answer_variable"
    assert hint is not None


def test_classify_session_error():
    """PyDoughSessionException → 'session' with a non-None hint."""
    e = PyDoughSessionException("No active graph set in PyDough session.")
    error_type, _, hint = _classify_error(e)
    assert error_type == "session"
    assert hint is not None


def test_classify_type_error():
    """PyDoughTypeException → 'type_error' with a non-None hint."""
    e = PyDoughTypeException("Expected numeric, got string")
    error_type, _, hint = _classify_error(e)
    assert error_type == "type_error"
    assert hint is not None


def test_classify_unsupported_operation():
    """NotImplementedError → 'unsupported_operation' with a non-None hint."""
    e = NotImplementedError("NDISTINCT in correlated context not supported")
    error_type, _, hint = _classify_error(e)
    assert error_type == "unsupported_operation"
    assert hint is not None


def test_classify_cross_without_lhs():
    """PyDoughSQLException mentioning CROSS → 'cross_without_lhs'."""
    e = PyDoughSQLException("Cannot use CROSS(collection) without a left-hand side")
    error_type, _, hint = _classify_error(e)
    assert error_type == "cross_without_lhs"
    assert hint is not None


def test_classify_sql_error():
    """PyDoughSQLException without CROSS → 'sql_error'."""
    e = PyDoughSQLException("SQL rewrite failed: ambiguous column reference")
    error_type, _, hint = _classify_error(e)
    assert error_type == "sql_error"
    assert hint is not None


def test_classify_plural_in_calculate():
    """Plural expression in CALCULATE → 'plural_in_calculate' with a non-None hint."""
    e = PyDoughQDAGException(
        "Expected all terms in CALCULATE(...) to be singular, "
        "but encountered a plural expression: JOIN_STRINGS(', ', procedures.description)"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "plural_in_calculate"
    assert hint is not None
    assert "singular" in hint.lower() or "SINGULAR" in hint


def test_classify_collection_as_expression():
    """'Expected an expression, but received a collection' → 'collection_as_expression'."""
    e = PyDoughQDAGException(
        "Expected an expression, but received a collection: "
        "conditions.WHERE(...).procedures.CALCULATE(...)"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "collection_as_expression"
    assert hint is not None
    assert "CALCULATE" in hint or "scalar" in hint.lower()


def test_classify_bad_window_per():
    """'per' string parsing error → 'bad_window_per' with a non-None hint."""
    e = PyDoughUnqualifiedException(
        "Error while parsing 'per' string of RANKING(by=key.DESC(), per='nations') "
        "in context customers (unrecognized ancestor 'nations')"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "bad_window_per"
    assert hint is not None
    assert "per=" in hint


def test_classify_downstream_conflict():
    """Name ambiguity between current and ancestor context → 'downstream_conflict'."""
    e = PyDoughQDAGException(
        "Unclear whether 'key' refers to a term of the current context "
        "or ancestor of collection nations.customers"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "downstream_conflict"
    assert hint is not None


def test_classify_invalid_operator_args():
    """Wrong operator argument types → 'invalid_operator_args'."""
    e = PyDoughQDAGException(
        "Invalid operator invocation LOWER('key', 'extra'): expected 1 argument, got 2"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "invalid_operator_args"
    assert hint is not None


def test_classify_type_inference_fail():
    """Return type inference failure also maps to 'invalid_operator_args'."""
    e = PyDoughQDAGException(
        "Unable to infer the return type of operator invocation CUSTOM_UDF(x, y): "
        "incompatible argument types"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "invalid_operator_args"
    assert hint is not None


def test_classify_metadata_error():
    """PyDoughMetadataException → 'metadata_error' with a non-None hint."""
    e = PyDoughMetadataException("Collection 'nonexistent' not found in graph TPCH")
    error_type, _, hint = _classify_error(e)
    assert error_type == "metadata_error"
    assert hint is not None


def test_classify_not_callable():
    """PyDoughUnqualifiedException 'not callable' → 'not_callable' with a non-None hint."""
    e = PyDoughUnqualifiedException(
        "PyDough object 'nations.key' is not callable. "
        "Did you mean to access an attribute or method?"
    )
    error_type, _, hint = _classify_error(e)
    assert error_type == "not_callable"
    assert hint is not None


def test_classify_qdag_error_without_suggestion():
    """PyDoughQDAGException without 'Did you mean:' → 'qdag_error' with no hint."""
    e = PyDoughQDAGException("Cardinality mismatch: expected singular context")
    error_type, _, hint = _classify_error(e)
    assert error_type == "qdag_error"
    assert hint is None


def test_classify_generic_exception():
    """An arbitrary Exception → 'generic' with no hint."""
    e = RuntimeError("something unexpected")
    error_type, _, hint = _classify_error(e)
    assert error_type == "generic"
    assert hint is None


def test_classify_all_types_return_dict_details():
    """Every error type returns a dict for details, never None."""
    exceptions = [
        SyntaxError("x"),
        PyDoughUnqualifiedException("answer variable 'result' not found"),
        PyDoughUnqualifiedException(
            "Error while parsing 'per' string of RANKING in context x (unrecognized ancestor)"
        ),
        PyDoughUnqualifiedException("PyDough object x is not callable."),
        PyDoughSessionException("no session"),
        PyDoughTypeException("bad type"),
        NotImplementedError("not supported"),
        PyDoughSQLException("CROSS without lhs"),
        PyDoughSQLException("generic sql fail"),
        PyDoughQDAGException("no suggestion here"),
        PyDoughQDAGException(
            "Unclear whether 'x' refers to a term of the current context or ancestor"
        ),
        PyDoughQDAGException("Invalid operator invocation FOO(x): bad args"),
        PyDoughQDAGException(
            "Unable to infer the return type of operator invocation FOO(x): error"
        ),
        PyDoughQDAGException("Expected an expression, but received a collection: x"),
        PyDoughQDAGException(
            "Expected all terms to be singular, but encountered a plural expression: x"
        ),
        PyDoughMetadataException("collection not found"),
        RuntimeError("unexpected"),
    ]
    for e in exceptions:
        _, details, _ = _classify_error(e)
        assert isinstance(details, dict), (
            f"details is not a dict for {type(e).__name__}"
        )


def test_error_md_no_hint_blockquote_for_generic():
    """Generic errors with no hint produce no blockquote in markdown."""
    from pydough.exploration.explain_llm import _error_payload, _render_md

    result = _error_payload(RuntimeError("something unexpected"))
    md = _render_md(result)
    assert result["hint"] is None
    assert "> " not in md


def test_md_invalid_format_raises(tpch_session: PyDoughSession) -> None:
    """Passing an unrecognised format raises ValueError."""
    node: UnqualifiedNode = pydough.from_string(
        "result = nations.CALCULATE(key, name)", session=tpch_session
    )
    with pytest.raises(ValueError, match="format"):
        pydough.explain_llm(node, session=tpch_session, format="xml")


# ---------------------------------------------------------------------------
# Reference-file tests — full JSON + markdown output per scenario
# ---------------------------------------------------------------------------
#
# Reference data lives in tests/test_explain_llm_refsols/.  For each scenario
# there are two files:
#
#   <name>.json  — contains:
#     {
#       "pydough":        "<code string passed to pydough.from_string>",
#       "explain_output": { <full explain_llm JSON dict> },
#       "explain_output_md": "<name>.md"   the MD version filename
#     }
#
#   <name>.md    — full explain_llm markdown output stored as actual markdown
#
# To add a new scenario: add a pytest.param to refsol_scenario below, then run:
#   PYDOUGH_UPDATE_TESTS=1 uv run pytest tests/test_explain_llm.py -k refsol

_REFSOL_DIR = Path(__file__).parent / "test_explain_llm_refsols"


@pytest.fixture(
    params=[
        pytest.param(
            ("nations_calculate", "result = nations.CALCULATE(key, name)"),
            id="nations_calculate",
        ),
        pytest.param(
            (
                "nations_where_calculate",
                "result = nations.WHERE(key > 5).CALCULATE(key, name)",
            ),
            id="nations_where_calculate",
        ),
        pytest.param(
            ("nations_where_only", "result = nations.WHERE(key > 5)"),
            id="nations_where_only",
        ),
        pytest.param(
            (
                "nations_where_and_conditions",
                "result = nations.WHERE((key > 5) & (key < 20))",
            ),
            id="nations_where_and_conditions",
        ),
        pytest.param(
            ("nations_order_by", "result = nations.ORDER_BY(name.ASC())"),
            id="nations_order_by",
        ),
        pytest.param(
            (
                "customers_count_orders",
                "result = customers.CALCULATE(key, n_orders=COUNT(orders))",
            ),
            id="customers_count_orders",
        ),
        pytest.param(
            (
                "nations_count_customers",
                "result = nations.CALCULATE(n=COUNT(customers))",
            ),
            id="nations_count_customers",
        ),
        pytest.param(
            ("nations_cross_regions", "result = nations.CROSS(regions)"),
            id="nations_cross_regions",
        ),
        pytest.param(
            ("nations_topk", "result = nations.TOP_K(5, by=name.ASC())"),
            id="nations_topk",
        ),
        pytest.param(
            (
                "nations_calculate_topk",
                "result = nations.CALCULATE(name=name, n=COUNT(customers)).TOP_K(1, by=n.DESC())",
            ),
            id="nations_calculate_topk",
        ),
        pytest.param(
            (
                "customers_partition_by_segment",
                "result = customers.PARTITION(name='g', by=market_segment).CALCULATE(market_segment=market_segment, n=COUNT(customers))",
            ),
            id="customers_partition_by_segment",
        ),
        pytest.param(
            (
                "customers_orders_subcollection",
                "result = customers.WHERE(market_segment == 'BUILDING').orders.CALCULATE(key=key)",
            ),
            id="customers_orders_subcollection",
        ),
        pytest.param(
            (
                "customers_where_orders_where",
                "result = customers.WHERE(market_segment == 'BUILDING').orders.WHERE(total_price > 1000).CALCULATE(key=key)",
            ),
            id="customers_where_orders_where",
        ),
        pytest.param(
            (
                "customers_singular_nation_name",
                "result = customers.CALCULATE(nation_name=nation.SINGULAR().name)",
            ),
            id="customers_singular_nation_name",
        ),
        pytest.param(
            (
                "customers_join_strings",
                "result = customers.CALCULATE(full=JOIN_STRINGS(' ', name, phone))",
            ),
            id="customers_join_strings",
        ),
        pytest.param(
            (
                "customers_pre_post_calculate_where",
                "result = customers.WHERE(market_segment == 'BUILDING').CALCULATE(name=name, n=COUNT(orders)).WHERE(RANKING(by=n.ASC()) == 1).CALCULATE(name)",
            ),
            id="customers_pre_post_calculate_where",
        ),
        pytest.param(
            (
                "customers_orders_ranking_where",
                "result = customers.orders.WHERE(RANKING(by=total_price.DESC(), per='customers') == 1).CALCULATE(key=key)",
            ),
            id="customers_orders_ranking_where",
        ),
        pytest.param(
            (
                "nations_where_partition",
                "result = nations.WHERE(key > 5).PARTITION(name='g', by=name).CALCULATE(name=name)",
            ),
            id="nations_where_partition",
        ),
        pytest.param(
            (
                "nations_iff_partition",
                "result = nations.WHERE(key > 5).CALCULATE(tier=IFF(key > 15, 'high', 'low')).PARTITION(name='g', by=tier).CALCULATE(tier=tier, n=COUNT(nations)).TOP_K(1, by=n.DESC()).CALCULATE(tier)",
            ),
            id="nations_iff_partition",
        ),
        pytest.param(
            (
                "customers_where_orders_partition",
                "result = customers.WHERE(market_segment == 'BUILDING').orders.PARTITION(name='g', by=order_status).CALCULATE(order_status=order_status, n=COUNT(orders)).TOP_K(1, by=n.DESC()).CALCULATE(order_status)",
            ),
            id="customers_where_orders_partition",
        ),
        pytest.param(
            (
                "customers_orders_lines_partition",
                "result = customers.WHERE(market_segment == 'BUILDING').orders.lines.PARTITION(name='g', by=return_flag).CALCULATE(return_flag=return_flag)",
            ),
            id="customers_orders_lines_partition",
        ),
        pytest.param(
            (
                "global_count_nations_where",
                "result = CALCULATE(n=COUNT(nations.WHERE(key > 5)))",
            ),
            id="global_count_nations_where",
        ),
        pytest.param(
            ("error_unrecognized_term", "result = nations.WHERE(naem == 'ASIA')"),
            id="error_unrecognized_term",
        ),
        pytest.param(
            ("error_expression_not_collection", "result = nations.key"),
            id="error_expression_not_collection",
        ),
    ]
)
def refsol_scenario(request) -> tuple[str, str]:
    """
    Parametrized fixture that yields ``(name, code)`` for each scenario.

    Returns:
        A ``(name, code)`` tuple where ``name`` is also the stem of the
        per-scenario reference files in ``_REFSOL_DIR``.
    """
    return request.param


def _run_explain_json_str(code: str, session: PyDoughSession) -> dict:
    """
    Evaluates a PyDough code string and returns the ``explain_llm`` JSON dict.

    Args:
        `code`: a PyDough code string of the form ``result = <expression>``.
        `session`: the PyDough session used for graph context and qualification.

    Returns:
        The JSON-serialisable dict produced by ``pydough.explain_llm``.
    """
    node: UnqualifiedNode = pydough.from_string(code, session=session)
    return cast(dict, pydough.explain_llm(node, session=session))


def _run_explain_md_str(code: str, session: PyDoughSession) -> str:
    """
    Evaluates a PyDough code string and returns the ``explain_llm`` markdown.

    Args:
        `code`: a PyDough code string of the form ``result = <expression>``.
        `session`: the PyDough session used for graph context and qualification.

    Returns:
        The markdown string produced by ``pydough.explain_llm(format="md")``.
    """
    node: UnqualifiedNode = pydough.from_string(code, session=session)
    return cast(str, pydough.explain_llm(node, session=session, format="md"))


def test_explain_llm_refsol_json(
    refsol_scenario: tuple[str, str],
    tpch_session: PyDoughSession,
    update_tests: bool,
) -> None:
    """
    Full JSON output matches the ``explain_output`` field in the per-scenario
    JSON reference file.

    When ``update_tests`` is ``True``, overwrites the reference file with the
    current output, co-locating the PyDough code, the JSON output, and the
    path to the sibling markdown file.

    Args:
        `refsol_scenario`: ``(name, code)`` tuple for the scenario.
        `tpch_session`: the PyDough session used for evaluation.
        `update_tests`: when ``True``, writes rather than compares.
    """
    name, code = refsol_scenario
    result = _run_explain_json_str(code, tpch_session)
    json_path = _REFSOL_DIR / f"{name}.json"
    if update_tests:
        _REFSOL_DIR.mkdir(exist_ok=True)
        entry = {
            "pydough": code,
            "explain_output": result,
            "explain_output_md": f"{name}.md",
        }
        json_path.write_text(json.dumps(entry, indent=2) + "\n")
    else:
        entry = json.loads(json_path.read_text())
        assert result == entry["explain_output"], (
            f"explain_llm JSON for '{name}' differs from the reference "
            f"file. Re-run with PYDOUGH_UPDATE_TESTS=1 to regenerate."
        )


def test_explain_llm_refsol_md(
    refsol_scenario: tuple[str, str],
    tpch_session: PyDoughSession,
    update_tests: bool,
) -> None:
    """
    Full markdown output matches the sibling ``.md`` reference file whose path
    is recorded in the per-scenario JSON file under ``"explain_output_md"``.

    When ``update_tests`` is ``True``, overwrites the markdown file with the
    current output.

    Args:
        `refsol_scenario`: ``(name, code)`` tuple for the scenario.
        `tpch_session`: the PyDough session used for evaluation.
        `update_tests`: when ``True``, writes rather than compares.
    """
    name, code = refsol_scenario
    result = _run_explain_md_str(code, tpch_session)
    json_path = _REFSOL_DIR / f"{name}.json"
    if update_tests:
        _REFSOL_DIR.mkdir(exist_ok=True)
        md_filename = f"{name}.md"
        md_path = _REFSOL_DIR / md_filename
        md_path.write_text(result + "\n")
        # Keep the JSON file's md pointer in sync if it already exists.
        if json_path.exists():
            entry = json.loads(json_path.read_text())
            entry["explain_output_md"] = md_filename
            json_path.write_text(json.dumps(entry, indent=2) + "\n")
    else:
        entry = json.loads(json_path.read_text())
        md_path = _REFSOL_DIR / entry["explain_output_md"]
        expected = md_path.read_text().rstrip("\n")
        assert result == expected, (
            f"explain_llm markdown for '{name}' differs from the "
            f"reference file. Re-run with PYDOUGH_UPDATE_TESTS=1 to regenerate."
        )
