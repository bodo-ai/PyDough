from fastapi import Body, FastAPI
from lookup_table import LOOKUP_TABLE
from pydantic import BaseModel

app: FastAPI = FastAPI()


# defines
class EvaluateRequest(BaseModel):
    column_reference: str
    predicate: list[str | int | float]
    mode: str = "dynamic"
    dry_run: bool = False


class RequestPayload(BaseModel):
    items: list[EvaluateRequest]
    expression_format: dict[str, str] = {"name": "linear", "version": "0.2.0"}


@app.post("/v1/predicates/evaluate")
def evaluate(request: dict = Body(...)):
    return {
        "result": "SUCCESS",
        "decision": {"strategy": "values", "reason": "mock"},
        "predicate_hash": "mockhash",
        "encryption_mode": "clear",
        "materialization": {
            "type": "literal",
            "operator": "IN",
            "values": [1, 2],
            "count": 2,
        },
    }


@app.post("/v1/predicates/batch-evaluate")
def batch_evaluate(request: RequestPayload):
    responses = []
    for item in request.items:
        key = (item.column_reference, tuple(item.predicate))
        print("USING KEY: ", key)
        response = LOOKUP_TABLE.get(key, {"result": "KEY NOT FOUND"})
        responses.append(response)

    return {"result": "SUCCESS", "items": responses}
