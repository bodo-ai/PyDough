"""
Unit tests for the PyDough mask server module.
"""

import io
import re
from contextlib import redirect_stdout

import pytest

from pydough.mask_server.mask_server import (
    MaskServerInfo,
    MaskServerInput,
    MaskServerOutput,
    MaskServerResponse,
)


@pytest.mark.server
@pytest.mark.parametrize(
    "token, batch, answer",
    [
        pytest.param(
            None,
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 0],
                ),
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                ),
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db/orders",
                    column_name="order_date",
                    expression=[
                        "AND",
                        2,
                        "LTE",
                        2,
                        "2025-01-01",
                        "__col__",
                        "LTE",
                        2,
                        "__col__",
                        "2025-02-01",
                    ],
                ),
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db/tbl",
                    column_name="col",
                    expression=["GT", 2, "__col__", 45.67],
                ),
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["NOT_EQUAL", 2, "__col__", "LOWER", 1, "Smith"],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.NOT_IN_ARRAY,
                    payload=[
                        "value1",
                        "value2",
                        "value3",
                    ],
                ),
                MaskServerOutput(
                    response_case=MaskServerResponse.UNSUPPORTED,
                    payload=None,
                ),
                MaskServerOutput(
                    response_case=MaskServerResponse.IN_ARRAY,
                    payload=[
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-03",
                        "2025-01-04",
                        "2025-01-05",
                    ],
                ),
                MaskServerOutput(
                    response_case=MaskServerResponse.UNSUPPORTED,
                    payload=None,
                ),
                MaskServerOutput(
                    response_case=MaskServerResponse.NOT_IN_ARRAY, payload=["smith"]
                ),
            ],
            id="alternated_supported_response",
        ),
        pytest.param(
            None,
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 0],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.NOT_IN_ARRAY,
                    payload=[
                        "value1",
                        "value2",
                        "value3",
                    ],
                ),
            ],
            id="single_supported_response",
        ),
        pytest.param(
            None,
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.UNSUPPORTED,
                    payload=None,
                ),
            ],
            id="single_unsupported_response",
        ),
        pytest.param(
            "test-token-123",
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.orders",
                    column_name="order_date",
                    expression=[
                        "AND",
                        2,
                        "LTE",
                        2,
                        "2025-01-01",
                        "__col__",
                        "LTE",
                        2,
                        "__col__",
                        "2025-02-01",
                    ],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.IN_ARRAY,
                    payload=[
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-03",
                        "2025-01-04",
                        "2025-01-05",
                    ],
                ),
            ],
            id="with_token",
        ),
        pytest.param(
            "test-token-123",
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["NOT_EQUAL", 2, "__col__", True],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.IN_ARRAY,
                    payload=[False],
                ),
            ],
            id="booleans",
        ),
        pytest.param(
            None,
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["LT", 2, "__col__", "123.654445"],
                ),
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=[
                        "AND",
                        2,
                        "NOT_EQUAL",
                        2,
                        "__col__",
                        None,
                        "GT",
                        2,
                        "__col__",
                        "$45.00",
                    ],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.IN_ARRAY,
                    payload=["123.121123", "123.654444", "123.654445"],
                ),
                MaskServerOutput(
                    response_case=MaskServerResponse.NOT_IN_ARRAY,
                    payload=[None, "$44.50", "$43.20", "$44.99"],
                ),
            ],
            id="decimal_and_money",
        ),
        pytest.param(
            None,
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=[
                        "OR",
                        2,
                        "AND",
                        2,
                        "REGEXP",
                        2,
                        "__col__",
                        "^[A-Z][a-z]+$",
                        "NOT_EQUAL",
                        2,
                        "__col__",
                        "SGVsbG9Xb3JsZA==",
                        "EQUAL",
                        2,
                        "__col__",
                        '"Hello World"',
                    ],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerResponse.IN_ARRAY,
                    payload=['"Hello"', "HelloWorld", "SGVsbG9Xb3JsZA=="],
                ),
            ],
            id="nested_string",
        ),
    ],
)
def test_mock_mask_server(
    token: str,
    batch: list[MaskServerInput],
    answer: list[MaskServerOutput],
    mock_server_setup,
) -> None:
    """
    Testing that the MaskServer can successfully create a request payload
    for the mock server and parse the response correctly.
    """

    mask_server: MaskServerInfo = MaskServerInfo(
        base_url="http://localhost:8000", token=token
    )

    # Capture stdout to avoid polluting the console with logging calls
    with redirect_stdout(io.StringIO()):
        # Doing the request
        response: list[MaskServerOutput] = mask_server.simplify_simple_expression_batch(
            batch=batch,
            dry_run=False,
        )

    assert response == answer, (
        f"Mismatch between the response {response!r} and the answer {answer!r}"
    )


@pytest.mark.server
@pytest.mark.parametrize(
    "base_url, token, batch, error_msg",
    [
        pytest.param(
            "http://localhost:8000",
            None,
            [],
            "Batch cannot be empty.",
            id="empty_list_request",
        ),
        pytest.param(
            "http://localhost:8000",
            "bad_token_123",
            [
                MaskServerInput(
                    dataset_id="dummy_server",
                    table_path="db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Bad response 401: Unauthorized request",
            id="wrong_token",
        ),
    ],
)
def test_mock_mask_server_errors(
    base_url: str,
    token: str | None,
    batch: list[MaskServerInput],
    error_msg: str,
    mock_server_setup,
) -> None:
    """
    Testing that the MaskServer raises an exception with the expected error message
    """
    with pytest.raises(Exception, match=re.escape(error_msg)):
        mask_server: MaskServerInfo = MaskServerInfo(base_url=base_url, token=token)
        mask_server.connection.set_timeout(0.5)

        # Doing the request
        mask_server.simplify_simple_expression_batch(
            batch=batch,
            dry_run=False,
        )
