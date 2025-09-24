"""
Unit tests for the PyDough mask server module.
"""

import re

import pytest

from pydough.mask_server.mask_server import (
    MaskServer,
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
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 0],
                ),
                MaskServerInput(
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                ),
                MaskServerInput(
                    table_path="srv.db.orders",
                    column_name="order_date",
                    expression=["BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"],
                ),
                MaskServerInput(
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["GT", 2, "__col__", 100],
                ),
                MaskServerInput(
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["NOT_EQUAL", 2, "__col__", 0],
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
                    response_case=MaskServerResponse.NOT_IN_ARRAY, payload=[0]
                ),
            ],
            id="alternated_supported_response",
        ),
        pytest.param(
            None,
            [
                MaskServerInput(
                    table_path="srv.db.tbl",
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
                    table_path="srv.db.tbl",
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
                    table_path="srv.db.orders",
                    column_name="order_date",
                    expression=["BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"],
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

    mask_server: MaskServer = MaskServer(base_url="http://localhost:8000", token=token)

    # Doing the request
    response: list[MaskServerOutput] = mask_server.simplify_simple_expression_batch(
        batch=batch,
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
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Bad response 401: Unauthorized request",
            id="wrong_token",
        ),
        pytest.param(
            "http://127.168.1.1:8000",
            None,
            [
                MaskServerInput(
                    table_path="srv.db.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Request failed: timed out",
            id="wrong_url",
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
        mask_server: MaskServer = MaskServer(base_url=base_url, token=token)
        mask_server.connection.set_timeout(0.5)

        # Doing the request
        mask_server.simplify_simple_expression_batch(
            batch=batch,
        )
