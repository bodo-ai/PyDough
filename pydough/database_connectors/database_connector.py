"""
Defines the abstract interfaces for any database connector.
These require the ability to connect to a database, execute a query,
and return the results.

TODO: Add metadata support.
"""
# Copyright (C) 2024 Bodo Inc. All rights reserved.

from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self, *args, **kwargs) -> "DatabaseCursor":
        """Connect to the database by providing the required
        connection parameters. Each database connector may
        accept different parameters, including potentially
        optional configuration parameters, but the most common
        case will be a single connection string.

        Returns:
            DatabaseCursor: _description_
        """

    # TODO: Extend the interface.


class DatabaseCursor(ABC):
    """
    A thin wrapper around a database cursor object
    that provides a common interface for executing
    queries on a database.
    """

    @abstractmethod
    def execute(self, sql: str, parameters=(), /) -> "DatabaseResult":
        """Execute a query on the database.

        Args:
            query (str): The query to execute.

        Returns:
            DatabaseResult: The result of the query.
        """

    # TODO: Extend the interface.


class DatabaseResult(ABC):
    """
    A thin wrapper around a database result object
    that provides a common interface for fetching
    results from a database query.
    """

    # TODO: Determine interface.
