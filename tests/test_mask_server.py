"""
Unit tests for the PyDough mask server module.
"""

import pytest

from pydough.mask_server.mask_server import (
    MaskServer,
    MaskServerInput,
    MaskServerOutput,
    MaskServerReponse,
)
from pydough.mask_server.server_connection import RequestMethod


@pytest.mark.execute
@pytest.mark.server
@pytest.mark.parametrize(
    "base_url, path, method, batch, answer",
    [
        pytest.param(
            "http://localhost:8000",
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
                    expression=["EQUAL", 2, "__col__", 1],
                ),
                MaskServerInput(
                    table_path="srv.analytics.tbl",
                    column_name="col",
                    expression=["EQUAL", 2, "__col__", 2],
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
                    response_case=MaskServerReponse.IN_ARRAY,
                    payload=[
                        "value1",
                        "value2",
                        "value3",
                        "value4",
                        "value5",
                    ],
                ),
                MaskServerOutput(
                    response_case=MaskServerReponse.UNSUPPORTED,
                    payload=None,
                ),
            ],
            id="mock-server-1",
        ),
    ],
)
def test_mask_server(
    base_url: str,
    path: str,
    method: RequestMethod,
    batch: list[MaskServerInput],
    answer: list[MaskServerOutput],
    setup_mock_server,
) -> None:
    """
    Testing that the MaskServer can successfully create a request payload
    for the mock server and parse the response correctly.
    """

    mask_server = MaskServer(base_url=base_url)

    # Doing the request
    response = mask_server.simplify_simple_expression_batch(
        batch=batch,
        path=path,
        method=method,
    )

    assert response == answer, (
        f"Mismatch between the response {response!r} and the answer {answer!r}"
    )
