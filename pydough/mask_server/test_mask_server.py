"""
This file is for testing the MaskServer (will be deleted later).
"""

from pydough.mask_server.mask_server import MaskServer, MaskServerInput
from pydough.mask_server.server_connection import RequestMethod


def main():
    base_url = "http://localhost:8000"

    mask_server = MaskServer(base_url=base_url)

    request_batch: list[MaskServerInput] = [
        MaskServerInput(
            table_path="srv.analytics.tbl",
            column_name="col",
            expression=["EQUAL", 2, "__col__", i],
        )
        for i in range(3)
    ]

    response = mask_server.simplify_simple_expression_batch(
        batch=request_batch,
        path="v1/predicates/batch-evaluate",
        method=RequestMethod.POST,
    )
    print("Response:", response)


if __name__ == "__main__":
    main()
