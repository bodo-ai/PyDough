"""
Definition of the base class for creating exceptions in PyDough.
"""

from typing import TYPE_CHECKING

from pydough.errors import PyDoughException, PyDoughQDAGException

if TYPE_CHECKING:
    from pydough.pydough_operators import PyDoughOperator
    from pydough.qdag import PyDoughCollectionQDAG, PyDoughExpressionQDAG


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

    def down_streaming_conflict(
        self, collection: "PyDoughCollectionQDAG", term_name: str
    ) -> PyDoughException:
        """
        Creates an exception for when a term accessed within a collection but
        it is unclear whether it is a term of the collection or a term
        downstreamed from an ancestor.

        Args:
            `collection`: The collection in which the term is being accessed.
            `term_name`: The name of the term that caused the ambiguity.

        Returns:
            An exception indicating the name access ambiguity.
        """
        return PyDoughQDAGException(
            f"Unclear whether {term_name!r} refers to a term of the current context or ancestor of collection {collection!r}"
        )

    def cardinality_error(
        self, collection: "PyDoughCollectionQDAG", expr: "PyDoughExpressionQDAG"
    ) -> PyDoughException:
        """
        Creates an exception for when a term is used within a context that
        should be singular with regards to the context, but it is plural.

        Args:
            `collection`: The collection in which the term is being accessed.
            `expr`: The PyDoughQDAG expression

        Returns:
            An exception indicating the cardinality error.
        """
        raise PyDoughQDAGException(
            f"Expected all terms in {collection.standalone_string} to be singular, but encountered a plural expression: {expr}"
        )

    def expected_collection(self, expr: object) -> PyDoughException:
        """
        Creates an exception for when a QDAG collection is expected but
        something else is found.
        """
        from pydough.qdag import PyDoughExpressionQDAG

        if isinstance(expr, PyDoughExpressionQDAG):
            return PyDoughQDAGException(
                f"Expected a collection, but received an expression: {expr}"
            )
        else:
            return PyDoughQDAGException(
                f"Expected a collection, but received {expr.__class__.__name__}:  {expr}"
            )

    def expected_expression(self, expr: object) -> PyDoughException:
        """
        Creates an exception for when a QDAG expression is expected but
        something else is found.
        """
        from pydough.qdag import PyDoughCollectionQDAG

        if isinstance(expr, PyDoughCollectionQDAG):
            return PyDoughQDAGException(
                f"Expected an expression, but received a collection: {expr}"
            )
        else:
            return PyDoughQDAGException(
                f"Expected an expression, but received {expr.__class__.__name__}:  {expr}"
            )

    def type_verification_fail(
        self, operator: "PyDoughOperator", args: list[object], message: str
    ) -> PyDoughException:
        """
        Creates an exception for when type verification fails for an operator.

        Args:
            `operator`: The operator that failed type verification.
            `args`: The arguments passed to the operator.
            `message`: The error message explaining the typing failure.

        Returns:
            An exception indicating the type verification failure.
        """
        arg_strings: list[str] = [str(arg) for arg in args]
        raise PyDoughQDAGException(
            f"Invalid operator invocation {operator.to_string(arg_strings)!r}: {message}"
        )

    def type_inference_fail(
        self, operator: "PyDoughOperator", args: list[object], message: str
    ) -> PyDoughException:
        """
        Creates an exception for when return type inference fails for an
        expression function operator.

        Args:
            `operator`: The operator that failed type inference.
            `args`: The arguments passed to the operator.
            `message`: The error message explaining the inference failure.

        Returns:
            An exception indicating the type inference failure.
        """
        arg_strings: list[str] = [str(arg) for arg in args]
        raise PyDoughQDAGException(
            f"Unable to infer the return type of operator invocation {operator.to_string(arg_strings)!r}: {message}"
        )

    def bad_columns(self, columns: object) -> PyDoughException:
        """
        Creates an exception for when the `columns` to `to_sql` or `to_df` is
        not valid.

        Args:
            `columns`: The columns argument that caused the error.

        Returns:
            An exception indicating the bad `columns` argument.
        """
        if isinstance(columns, list):
            for column in columns:
                if not isinstance(column, str):
                    return PyDoughQDAGException(
                        f"Expected `columns` argument to be a list of strings, found {column.__class__.__name__}"
                    )
            return PyDoughQDAGException(
                "Expected `columns` argument to be a non-empty list"
            )
        elif isinstance(columns, dict):
            for alias, column in columns.items():
                if not isinstance(alias, str):
                    return PyDoughQDAGException(
                        f"Expected `columns` argument to be a dictionary where the keys are strings, found {alias.__class__.__name__}"
                    )
                if not isinstance(column, str):
                    return PyDoughQDAGException(
                        f"Expected `columns` argument to be a dictionary where the values are strings, found {column.__class__.__name__}"
                    )
            return PyDoughQDAGException(
                "Expected `columns` argument to be a non-empty dictionary"
            )
        else:
            return PyDoughQDAGException(
                f"Expected `columns` argument to be a list or dictionary, found {columns.__class__.__name__}"
            )
