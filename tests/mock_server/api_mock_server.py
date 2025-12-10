"""
Mock FastAPI server for testing Mask Server interface.

This server provides endpoints to:
- Check server health with optional token authentication.
- Batch evaluate predicates using a lookup table for deterministic responses.

Intended for use in unit and integration tests.
"""

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

from .lookup_table import LOOKUP_TABLE

app: FastAPI = FastAPI()


class EvaluateRequest(BaseModel):
    dataset_id: str
    column_ref: dict[str, str]
    predicate: list[str | int | float | None | bool]
    output_mode: str
    mode: str
    dry_run: bool


class RequestPayload(BaseModel):
    items: list[EvaluateRequest]
    expression_format: dict[str, str] = {"name": "linear", "version": "0.2.0"}


def verify_token(request: Request):
    auth_header = request.headers.get("Authorization", None)

    if auth_header and auth_header != "Bearer test-token-123":
        raise HTTPException(status_code=401, detail="Unauthorized request")

    return True


@app.get("/health")
def health(request: Request, authorized: bool = Depends(verify_token)):
    return {"status": "ok"}


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(
    request: Request, payload: RequestPayload, authorized: bool = Depends(verify_token)
):
    responses: list[dict] = []
    successful_responses: int = 0
    # Process each item in the batch
    for item in payload.items:
        assert set(item.column_ref.keys()) == {
            "kind",
            "value",
        }, f"Invalid column_reference format in mock: {item.column_ref!r}."
        assert item.column_ref["kind"] == "fqn", "Only FQN kind is supported in mock."
        key = (item.dataset_id, item.column_ref["value"], tuple(item.predicate))
        table_result: tuple[str, list] | None = LOOKUP_TABLE.get(key, None)
        out_item: dict = {
            "index": payload.items.index(item) + 1,
        }
        if table_result is None:
            out_item["result"] = "ERROR"
        else:
            output_case, output_list = table_result
            out_item["SUCCESS"] = "SUCCESS"
            out_item["response"] = {
                "strategy": "early_stop",
                "records": [
                    {
                        "mode": "cell_encrypted",
                        "cell_encrypted": elem,
                    }
                    for elem in output_list
                ],
                "count": len(output_list),
                "stats": {"execution_time_ms": 42},
                "column_stats": None,
                "next_cursor": None,
                "metadata": {
                    "requested_output_mode": "cell_encrypted",
                    "actual_output_mode": "cell_encrypted",
                    "available_output_modes": ["cell_encrypted"],
                    "encryption_mode": None,
                    "dynamic_operator": output_case,
                },
            }
            # Don't include response in dry run case
            if item.dry_run:
                out_item["response"].pop("records")
            successful_responses += 1

        # Adding the new item to the batch output
        responses.append(out_item)

    result: str
    if successful_responses == len(payload.items):
        result = "SUCCESS"
    elif successful_responses == 0:
        result = "ERROR"
    else:
        result = "PARTIAL_FAILURE"
    return {"result": result, "items": responses}
