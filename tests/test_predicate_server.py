import pytest
from httpx import AsyncClient

from pydough.predicate_server.api_client import PredicateAPIClient
from pydough.predicate_server.batch_evaluate_response import BatchEvaluateResponse
from pydough.predicate_server.evaluate_request import (
    BatchEvaluateRequest,
    EvaluateRequest,
)
from pydough.predicate_server.evaluate_response import EvaluateResponse
from pydough.predicate_server.predicate_expression import PredicateExpression


@pytest.mark.predicate_api
async def test_client_with_mock():
    async with AsyncClient(app=app, base_url="http://test") as async_client:

        class TestClient(PredicateAPIClient):
            def __init__(self, async_client):
                self.async_client = async_client

            async def evaluate(self, request: EvaluateRequest):
                resp = await self.async_client.post(
                    "/v1/predicates/evaluate", json=request.to_dict()
                )
                return EvaluateResponse(resp.json())

            async def batch_evaluate(self, request: BatchEvaluateRequest):
                resp = await self.async_client.post(
                    "/v1/predicates/batch-evaluate", json=request.to_dict()
                )
                return BatchEvaluateResponse(resp.json())

        pred = PredicateExpression.from_list(["EQUAL", 2, "__col__", 123])
        eval_req = EvaluateRequest("srv.analytics.tbl.col", pred)

        client = TestClient(async_client)

        eval_resp = await client.evaluate(eval_req)
        assert eval_resp.result == "SUCCESS"
        assert eval_resp.materialization.values == [1, 2]

        batch_req = BatchEvaluateRequest()
        batch_req.add_item(eval_req)
        batch_req.add_item(eval_req)

        batch_resp = await client.batch_evaluate(batch_req)
        assert isinstance(batch_resp, BatchEvaluateResponse)
        assert len(batch_resp) == 2
        assert batch_resp.items[0].index == 0
