"""
PyDough implementation of a generic connection to database
by leveraging PEP 249 (Python Database API Specification v2.0).
https://peps.python.org/pep-0249/
"""
# Copyright (C) 2024 Bodo Inc. All rights reserved.

from builtins import module


class SupportedDatabase:
    """Supported Database in PyDough."""

    _import_error_msg: str
    _module: module

    def __init__(self, mod_name: str, import_error_msg: str):
        self.import_error_msg = import_error_msg
        try:
            self._module = __import__(mod_name)
        except ImportError:
            self._module = None

    def module(self):
        if self._module is None:
            raise ImportError(self.import_error_msg)
        return self._module


supported_databases = {
    "sqlite": SupportedDatabase(
        "sqlite3", "PyDough requires the sqlite3 module for SQLite databases."
    )
}


def connect(database: str, *args, **kwargs):
    """Connect to a database and return a connection object
    that is compliant with PEP 249.

    This function doesn't have much value and may be removed in the future,
    but it is added here to track which database(s) are supported/tested.

    Args:
        database (str): The name of the database to connect to.
        *args: Variable length argument list. These are passed to the
            underlying database driver.
        **kwargs: Arbitrary keyword arguments. These are passed to the
            underlying database driver.

    Returns:
        Connection: A connection object in the given implementation library.
    """
    if database not in supported_databases:
        raise ValueError(f"Unsupported database: {database}")
    return supported_databases[database].module().connect(*args, **kwargs)
