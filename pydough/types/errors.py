"""
Error-handling definitions for the types module.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

__all__ = ["PyDoughTypeException"]


class PyDoughTypeException(Exception):
    """Exception raised when there is an error relating to PyDough types, such
    as malformed inputs to a parametrized type or a string that cannot be
    parsed into a type.
    """
