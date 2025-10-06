"""
Connection module for the server. It contains the data structures
and the main class to interact with a server.
"""

__all__ = ["RequestMethod", "ServerConnection", "ServerRequest"]

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

import httpx
from httpx import Response


class RequestMethod(Enum):
    """
    Class to represent the type of request to the Server.
    """

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class ServerRequest:
    """
    Dataclass that represents a general server request
    """

    path: str = ""
    """
    The path to the endpoint.
    """

    payload: dict = field(default_factory=dict)
    """
    The payload request. This is the data to be sent in the request body.
    """

    method: RequestMethod = RequestMethod.GET
    """
    The HTTP method to use for the request.
    """

    headers: dict = field(default_factory=lambda: {"Content-Type": "application/json"})
    """
    Optional headers to include in the request.
    """


class ServerConnection:
    """
    Class that manages the connection to the server.
    """

    def __init__(self, base_url: str, token: str | None = None):
        """
        Initialize the server connection with the given base URL and token if
        given.

        Args:
            `base_url`: The URL of the mask server.
            `token`: The Token to use for authentication (optional).
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._client = httpx.Client(base_url=self.base_url)

    def set_timeout(self, timeout: float) -> None:
        """
        Set the timeout for the HTTP client.

        Args:
            `timeout`: The timeout value in seconds.
        """
        self._client.timeout = timeout

    def method_mapping(self, request_method: RequestMethod) -> Callable[..., Response]:
        """
        Map of endpoints to their request methods.

        Returns:
            A dictionary mapping endpoint strings to RequestMethod enums.
        """
        match request_method:
            case RequestMethod.GET:
                return self._client.get
            case RequestMethod.POST:
                return self._client.post
            case RequestMethod.PUT:
                return self._client.put
            case RequestMethod.DELETE:
                return self._client.delete
            case _:
                raise ValueError(f"Unsupported request method: {request_method}")

    def send_server_request(self, request: ServerRequest) -> dict:
        """
        Send a request to the server.

        Args:
            `request`: The request to send to the server.

        Returns:
            The response from the server as a dictionary.
        """

        try:
            method: Callable[..., Response] = self.method_mapping(request.method)

            headers = request.headers.copy() if request.headers else {}
            if self.token:
                headers.setdefault("Authorization", f"Bearer {self.token}")
            kwargs = {"headers": headers}

            # Choose params vs json depending on method
            if request.method in (RequestMethod.GET, RequestMethod.DELETE):
                kwargs["params"] = request.payload
            else:  # POST, PUT
                kwargs["json"] = request.payload

            server_response: Response = method(request.path, **kwargs)
            server_response.raise_for_status()  # goes to except HTTPStatusError

            return server_response.json()

        except httpx.RequestError as e:
            # Errors if something goes wrong before get a valid HTTP response.
            # such as network problems, DNS failure, etc.
            raise RuntimeError(f"Request failed: {e}") from e

        except httpx.HTTPStatusError as e:
            # HTTP error status codes (4xx, 5xx)
            try:
                # Parse JSON and get the "detail" field
                error_detail = e.response.json().get("detail", e.response.text)
            except ValueError:
                # Response body is not JSON
                error_detail = e.response.text
            raise RuntimeError(
                f"Bad response {e.response.status_code}: {error_detail}"
            ) from e
