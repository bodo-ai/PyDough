from fastapi import Body, FastAPI

app = FastAPI()


@app.post("/v1/predicates/evaluate")
async def evaluate(request: dict = Body(...)):
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
async def batch_evaluate(request: dict = Body(...)):
    return {
        "result": "SUCCESS",
        "items": [
            {
                "index": idx,
                "result": "SUCCESS",
                "decision": {"strategy": "values", "reason": "mock"},
                "predicate_hash": f"hash{idx}",
                "encryption_mode": "clear",
                "materialization": {
                    "type": "literal",
                    "operator": "IN",
                    "values": [idx],
                    "count": 1,
                },
            }
            for idx, _ in enumerate(request["items"])
        ],
    }
