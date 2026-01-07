"""
Interface for the mask server. This API includes the MaskServerInfo class and related
data structures including the MaskServerInput and MaskServerOutput dataclasses.
"""

__all__ = [
    "MaskServerInfo",
    "MaskServerInput",
    "MaskServerOutput",
    "MaskServerResponse",
]

import base64
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

from pydough.errors.error_utils import ValidSQLName
from pydough.logger import get_logger
from pydough.mask_server.server_connection import (
    RequestMethod,
    ServerConnection,
    ServerRequest,
)


class MaskServerResponse(Enum):
    """
    Enum to represent the type of response from the MaskServer.
    """

    IN_ARRAY = "IN_ARRAY"
    """
    The mask server returned an "IN" response.
    """

    NOT_IN_ARRAY = "NOT_IN_ARRAY"
    """
    The mask server returned an "NOT_IN" response.
    """

    UNSUPPORTED = "UNSUPPORTED"
    """
    The mask server returned an "UNSUPPORTED" response. Or the response is not 
    one of the supported cases.
    """


@dataclass
class MaskServerInput:
    """
    Input data structure for the MaskServer.
    """

    dataset_id: str
    """
    The dataset ID to use when querying the mask server.
    """

    table_path: str
    """
    The fully qualified SQL table path, given from the metadata.
    """

    column_name: str
    """
    The SQL column name, given from the metadata.
    """

    expression: list[str | int | float | None | bool]
    """
    The linear serialization of the predicate expression.
    """

    @property
    def fully_qualified_name(self) -> str:
        """
        Returns the fully qualified name of the column in the format
        'table_path/column_name', with `/` as the separator used to modify the
        `table_path` appropriately.
        """
        table_path_chunks: list[str] = ValidSQLName._split_identifier(self.table_path)
        return f"{'/'.join(table_path_chunks)}/{self.column_name}"


@dataclass
class MaskServerOutput:
    """
    Output data structure for the MaskServer.

    If the server returns an unsupported value, it returns an output with
    UNSUPPORTED + a None payload.
    """

    response_case: MaskServerResponse
    """
    The type of response from the server.
    """

    payload: Any
    """
    The payload of the response. This can be the result of the predicate evaluation
    or None if an error occurred.
    """


class MaskServerInfo:
    """
    The MaskServerInfo class is responsible for evaluating predicates against a
    given table and column. It interacts with an external mask server to
    perform the evaluation.
    """

    batch_evaluate_api_path: str = "v1/predicates/batch-evaluate"
    """
    The API path for batch evaluating predicates on the mask server.
    """

    def __init__(self, base_url: str, token: str | None = None):
        """
        Initialize the MaskServerInfo with the given server URL.

        Args:
            `base_url`: The URL of the mask server.
            `token`: Optional authentication token for the server.
        """
        self.connection: ServerConnection = ServerConnection(
            base_url=base_url, token=token
        )

    def get_server_response_case(self, response_metadata: dict) -> MaskServerResponse:
        """
        Mapping from server response strings to MaskServerResponse enum values.

        Args:
            `response_metadata`: The metadata field from the server response.
        Returns:
            The corresponding MaskServerResponse enum value.
        """
        if response_metadata.get("dynamic_operator", None) == "IN":
            match response_metadata.get("representation", None):
                case "IN" | None:
                    return MaskServerResponse.IN_ARRAY
                case "NOT_IN":
                    return MaskServerResponse.NOT_IN_ARRAY
                case _:
                    return MaskServerResponse.UNSUPPORTED
        return MaskServerResponse.UNSUPPORTED

    def simplify_simple_expression_batch(
        self,
        batch: list[MaskServerInput],
        dry_run: bool,
    ) -> list[MaskServerOutput]:
        """
        Sends a batch of predicate expressions to the mask server for evaluation.

        Each input in the batch specifies a table, column, and predicate
        expression.The method constructs a request, sends it to the server,
        and parses the response into a list of MaskServerOutput objects, each
        indicating the server's decision for the corresponding input.

        Args:
            `batch`: The list of inputs to be sent to the server.
            `dry_run`: Whether to perform a dry run or not.

        Returns:
            An output list containing the response case and payload.
        """

        # Obtain the `hard_limit` (the maximum number of items that can be
        # returned for each predicate) from the environment variable. Set the
        # default to 1000 if the variable is not set or invalid.
        hard_limit: int
        try:
            hard_limit = int(os.environ.get("PYDOUGH_MASK_SERVER_HARD_LIMIT", "1000"))
        except Exception:
            hard_limit = 1000

        # Log the batch request
        pyd_logger = get_logger(__name__)
        if dry_run:
            pyd_logger.info(
                f"Batch request (dry run) to Mask Server ({len(batch)} items):"
            )
        else:
            pyd_logger.info(f"Batch request to Mask Server ({len(batch)} items):")
        for idx, item in enumerate(batch):
            pyd_logger.info(
                f"({idx + 1}) {item.fully_qualified_name}: {item.expression}"
            )

        assert batch != [], "Batch cannot be empty."

        # Break down the batch into multiple requests if necessary, send them to
        # the server, and collect the results.
        result: list[MaskServerOutput] = []
        for req in self.generate_requests(batch, dry_run, hard_limit):
            response_json = self.connection.send_server_request(req)
            result.extend(self.generate_result(response_json))

        return result

    def generate_requests(
        self,
        batch: list[MaskServerInput],
        dry_run: bool,
        hard_limit: int,
    ) -> list[ServerRequest]:
        """
        Generate a server request from the given batch of server inputs.

        Args:
            `batch`: A list of MaskServerInput objects.
            `dry_run`: Whether the request is a dry run or not.
            `hard_limit`: The maximum number of items that can be returned for
            each predicate.

        Returns:
            A server request including payload to be sent. Returned as a list
            in case multiple requests are needed.

        Example payload:
        ```
        {
            "items": [
                {
                    "dataset_id": "snowflake.bodo.blah_blah_blah",
                    "column_ref": {"kind": "fqn", "value": "db/schema/table/name"},
                    "predicate": ["EQUAL", 2, "__col__", 1],
                    "mode": "dynamic",
                    "predicate_format": "linear_with_arity",
                    "output_mode": "cell_encrypted",
                    "dry_run": true,
                    "limits": {"dedup": True},
                },
                ...
            ],
            "expression_format": {"name": "linear", "version": "0.2.0"},
            "hard_limit": 1000,
        }
        ```
        """
        result: list[ServerRequest] = []
        step_size = 16  # Max 16 items per batch request
        for start_idx in range(0, len(batch), step_size):
            # Create the payload for the overall batch request, then populate the
            # items list with each individual request.
            payload: dict = {
                "items": [],
                "expression_format": {"name": "linear", "version": "0.2.0"},
                "hard_limit": hard_limit,
            }

            # Populate each individual request in the batch in the specified format.
            for idx in range(start_idx, min(start_idx + step_size, len(batch))):
                item: MaskServerInput = batch[idx]
                evaluate_request: dict = {
                    "dataset_id": item.dataset_id,
                    "column_ref": {
                        "kind": "fqn",
                        "value": item.fully_qualified_name,
                    },
                    "predicate": item.expression,
                    "output_mode": "cell_encrypted",
                    "mode": "dynamic",
                    "predicate_format": "linear_with_arity",
                    "dry_run": dry_run,
                    "limits": {"dedup": True},
                }
                payload["items"].append(evaluate_request)

            result.append(
                ServerRequest(
                    path=self.batch_evaluate_api_path,
                    payload=payload,
                    method=RequestMethod.POST,
                )
            )
        return result

    def generate_result(self, response_dict: dict) -> list[MaskServerOutput]:
        """
        Generate a list of server outputs from the server response of a batch
        request, either for a dry run or a normal run. On dry run requests, the
        `records` field will be absent.

        Args:
            `response_dict`: The response from the mask server.

        Example response:
        ```
        {
            "result": "SUCCESS",
            "items": [
                {
                    "index": 0,
                    "result": "SUCCESS",
                    "response": {
                        "strategy": ...,

                        "records": [
                            {
                                "mode": "cell_encrypted",
                                "cell_encrypted": "abcE1dsa",
                            }
                        ],

                        "count": ...,

                        "stats": ...,

                        "column_stats": ...,

                        "next_cursor": ...,

                        "metadata": {
                            "dynamic_operator": "IN",
                            ...
                        }
                    }
                },
                ...
            ]
        }
        ```

        Returns:
            A list of server outputs objects.
        """
        result: list[MaskServerOutput] = []

        for item in response_dict.get("items", []):
            # Case on whether operator is ERROR or not.
            # If ERROR, then response_case is unsupported and payload is None.
            # Otherwise, call self.get_server_response(operator) to get the
            # enum, store in a variable, then case on this variable to obtain
            # the payload.
            if item.get("result") == "ERROR":
                result.append(
                    MaskServerOutput(
                        response_case=MaskServerResponse.UNSUPPORTED,
                        payload=None,
                    )
                )
            else:
                response: dict = item["response"]
                if response.get("records", None) is None:
                    # In this case, it was a dry-run, and use a dummy value to
                    # indicate that it was successful.
                    result.append(
                        MaskServerOutput(
                            response_case=MaskServerResponse.IN_ARRAY,
                            payload=None,
                        )
                    )
                else:
                    # In this case, parse the response normally.
                    response_case: MaskServerResponse = self.get_server_response_case(
                        response["metadata"]
                    )

                    payload: Any = None

                    if response_case in (
                        MaskServerResponse.IN_ARRAY,
                        MaskServerResponse.NOT_IN_ARRAY,
                    ):
                        # If the response is an IN_ARRAY or NOT_IN_ARRAY,
                        # extract all the records to get the cell encrypted
                        # values, and decode them from base64.
                        payload = []
                        for record in response.get("records", []):
                            record_raw = record["cell_encrypted"]
                            if isinstance(record_raw, str):
                                padded = (
                                    record_raw + "=" * (4 - len(record_raw) % 4)
                                    if len(record_raw) % 4
                                    else record_raw
                                )
                                payload.append(base64.b64decode(padded).decode("utf-8"))
                            else:
                                payload.append(record_raw)

                    result.append(
                        MaskServerOutput(
                            response_case=response_case,
                            payload=payload,
                        )
                    )

        return result
