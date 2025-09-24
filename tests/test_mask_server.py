"""
Unit tests for the PyDough mask server module.
"""

import re

import pytest

from pydough.mask_server.mask_server import (
    MaskServer,
    MaskServerInput,
    MaskServerOutput,
    MaskServerReponse,
)
from pydough.mask_server.server_connection import RequestMethod


@pytest.mark.server
@pytest.mark.parametrize(
    "base_url, token, path, method, batch, answer",
    [
        pytest.param(
            "http://localhost:8000",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 0],
                ),
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                ),
                MaskServerInput(
                    table_path="srv.analytics.orders",
                    column_name="order_date",
                    expression=["BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"],
                ),
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["GT", 2, "__col__", 100],
                ),
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["NOT_EQUAL", 2, "__col__", 0],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerReponse.NOT_IN_ARRAY,
                    payload=[
                        "value1",
                        "value2",
                        "value3",
                    ],
                ),
                MaskServerOutput(
                    response_case=MaskServerReponse.UNSUPPORTED,
                    payload=None,
                ),
                MaskServerOutput(
                    response_case=MaskServerReponse.IN_ARRAY,
                    payload=[
                        "2025-01-01",
                        "2025-01-02",
                        "2025-01-03",
                        "2025-01-04",
                        "2025-01-05",
                    ],
                ),
                MaskServerOutput(
                    response_case=MaskServerReponse.UNSUPPORTED,
                    payload=None,
                ),
                MaskServerOutput(
                    response_case=MaskServerReponse.NOT_IN_ARRAY, payload=[0]
                ),
            ],
            id="alternated_supported_response",
        ),
        pytest.param(
            "http://localhost:8000",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 0],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerReponse.NOT_IN_ARRAY,
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
            "http://localhost:8000",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerReponse.UNSUPPORTED,
                    payload=None,
                ),
            ],
            id="single_unsupported_response",
        ),
        pytest.param(
            "http://localhost:8000",
            "test-token-123",
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.orders",
                    column_name="order_date",
                    expression=["BETWEEN", 3, "__col__", "2025-01-01", "2025-02-01"],
                ),
            ],
            [
                MaskServerOutput(
                    response_case=MaskServerReponse.IN_ARRAY,
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
def test_mask_server(
    base_url: str,
    token: str | None,
    path: str,
    method: RequestMethod,
    batch: list[MaskServerInput],
    answer: list[MaskServerOutput],
    mock_server_setup,
) -> None:
    """
    Testing that the MaskServer can successfully create a request payload
    for the mock server and parse the response correctly.
    """

    mask_server: MaskServer = MaskServer(base_url=base_url, token=token)

    # Doing the request
    response: list[MaskServerOutput] = mask_server.simplify_simple_expression_batch(
        batch=batch,
        path=path,
        method=method,
    )

    assert response == answer, (
        f"Mismatch between the response {response!r} and the answer {answer!r}"
    )


@pytest.mark.server
@pytest.mark.parametrize(
    "base_url, token, path, method, batch, error_msg",
    [
        pytest.param(
            "http://localhost:8000",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [],
            "Batch cannot be empty.",
            id="empty_list_request",
        ),
        pytest.param(
            "http://localhost:8000",
            "bad_token_123",
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Bad response 401: Unauthorized request",
            id="wrong_token",
        ),
        pytest.param(
            "http://localhost:8000",
            None,
            "v1/predicates/wrong-path",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Bad response 404: Not Found",
            id="wrong_path",
        ),
        pytest.param(
            "http://localhost:8080",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Request failed: [Errno 61] Connection refused",
            id="wrong_port",
        ),
        pytest.param(
            "http://12.34.56.78:8000",
            None,
            "v1/predicates/batch-evaluate",
            RequestMethod.POST,
            [
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["OR", 2, "__col__", 5],
                )
            ],
            "Request failed: timed out",
            id="wrong_url",
        ),
    ],
)
def test_mask_server_errors(
    base_url: str,
    token: str | None,
    path: str,
    method: RequestMethod,
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
            path=path,
            method=method,
        )
