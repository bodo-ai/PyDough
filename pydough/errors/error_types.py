"""
Definitions of various exception classes used within PyDough.
"""

__all__ = [
    "PyDoughMetadataException",
    "PyDoughQDAGException",
    "PyDoughTypeException",
    "PyDoughUnqualifiedException",
]


class PyDoughMetadataException(Exception):
    """
    Exception raised when there is an error relating to PyDough metadata, such
    as an error while parsing/validating the JSON or an ill-formed pattern.
    """


class PyDoughUnqualifiedException(Exception):
    """
    Exception raised when there is an error relating to the PyDough
    unqualified form, such as a Python object that cannot be coerced or an
    invalid use of a method that can be caught even without qualification.
    """


class PyDoughQDAGException(Exception):
    """
    Exception raised when there is an error relating to a PyDough QDAG, such
    as malformed arguments/structure, undefined term accesses, singular vs
    plural cardinality mismatches, or other errors that can be caught during
    qualification.
    """


class PyDoughTypeException(Exception):
    """
    Exception raised when there is an error relating to PyDough types, such
    as malformed inputs to a parametrized type or a string that cannot be
    parsed into a type.
    """
