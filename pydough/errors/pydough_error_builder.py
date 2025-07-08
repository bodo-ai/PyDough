"""
Definition of the base class for creating exceptions in PyDough.
"""

from typing import TYPE_CHECKING

from pydough.errors import PyDoughException, PyDoughQDAGException

if TYPE_CHECKING:
    from pydough.qdag import PyDoughCollectionQDAG


class PyDoughErrorBuilder:
    """
    Base class for creating exceptions in PyDough. This class provides an
    interface that the internals of PyDough will call to create various
    exceptions. An instance of this class is installed in the PyDough active
    session, telling PyDough how to create exceptions and what their messages
    should contain for most situations. A subclass can be created and installed
    into the session to customize the error messages.
    """

    def term_not_found(
        self, collection: "PyDoughCollectionQDAG", term_name: str
    ) -> PyDoughException:
        """
        Creates an exception for when a term is not found in the specified collection.

        Args:
            `collection`: The collection in which the term was not found.
            `term_name` The name of the term that was not found.
        Returns:
            An exception indicating that the term was not found.
        """
        return PyDoughQDAGException(
            collection.name_mismatch_error(term_name, atol=2, rtol=0.1, min_names=3)
        )
