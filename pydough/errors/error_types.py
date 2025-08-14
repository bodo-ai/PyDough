"""
Definitions of various exception classes used within PyDough.
"""

__all__ = [
    "PyDoughException",
    "PyDoughMetadataException",
    "PyDoughQDAGException",
    "PyDoughSQLException",
    "PyDoughSessionException",
    "PyDoughTestingException",
    "PyDoughTypeException",
    "PyDoughUnqualifiedException",
]


class PyDoughException(Exception):
    """
    Base class for all PyDough exceptions.
    """


class PyDoughSessionException(PyDoughException):
    """
    Exception raised when something goes wrong with the PyDough session or
    configs, such as assigning to a configuration that does not exist, or
    not mounting a graph or database to the session when they are needed,
    or issues with the setup of the database.
    """


class PyDoughMetadataException(PyDoughException):
    """
    Exception raised when there is an error relating to PyDough metadata, such
    as an error while parsing/validating the JSON or an ill-formed pattern.
    """


class PyDoughUnqualifiedException(PyDoughException):
    """
    Exception raised when there is an error relating to the PyDough
    unqualified form, such as a Python object that cannot be coerced or an
    invalid use of a method that can be caught even without qualification.
    """


class PyDoughQDAGException(PyDoughException):
    """
    Exception raised when there is an error relating to a PyDough QDAG, such
    as malformed arguments/structure, undefined term accesses, singular vs
    plural cardinality mismatches, or other errors that can be caught during
    qualification.
    """


class PyDoughTypeException(PyDoughException):
    """
    Exception raised when there is an error relating to PyDough types, such
    as malformed inputs to a parametrized type or a string that cannot be
    parsed into a type.
    """


class PyDoughSQLException(PyDoughException):
    """
    Exception caused by a malformation in the SQL that causes bugs during SQL
    generation, SQL rewrites/optimization or, or errors during SQL execution.
    """


class PyDoughTestingException(PyDoughException):
    """
    Exception raised within PyDough testing logic to indicate that something
    has gone wrong, e.g. when the AstNodeTestInfo classes are used incorrectly.
    """
