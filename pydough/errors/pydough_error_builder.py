"""
Definition of the base class for creating exceptions in PyDough.
"""

from typing import TYPE_CHECKING

from pydough.errors import (
    PyDoughException,
    PyDoughQDAGException,
    PyDoughSQLException,
    PyDoughUnqualifiedException,
)
from pydough.errors.error_utils import find_possible_name_matches

if TYPE_CHECKING:
    from pydough.pydough_operators import PyDoughOperator
    from pydough.qdag import PyDoughCollectionQDAG, PyDoughExpressionQDAG
    from pydough.relational import CallExpression
    from pydough.unqualified import UnqualifiedNode, UnqualifiedWindow


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
            collection.name_mismatch_error(
                term_name,
                atol=2,
                rtol=0.1,
                min_names=3,
                insert_cost=0.5,
                delete_cost=1.0,
                substitution_cost=1.0,
                capital_cost=0.1,
            )
        )

    def downstream_conflict(
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
        return PyDoughQDAGException(
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
        elif expr is None:
            return PyDoughQDAGException("Expected a collection, but received None")
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
        elif expr is None:
            return PyDoughQDAGException("Expected a collection, but received None")
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
        return PyDoughQDAGException(
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
        return PyDoughQDAGException(
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
            # If we reached this far, it is because the argument was an empty
            # list.
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
            # If we reached this far, it is because the argument was an empty
            # dictionary.
            return PyDoughQDAGException(
                "Expected `columns` argument to be a non-empty dictionary"
            )
        else:
            return PyDoughQDAGException(
                f"Expected `columns` argument to be a list or dictionary, found {columns.__class__.__name__}"
            )

    def sql_runtime_failure(
        self, sql: str, error: Exception, execute: bool
    ) -> PyDoughException:
        """
        Creates an exception for when a SQL query fails to execute at runtime
        or optimization.

        Args:
            `sql`: The SQL query that failed.
            `error`: The exception raised during the SQL execution or
            optimization.
            `execute`: Whether the failure occurred during execution (True) or
            optimization (False).

        Returns:
            An exception indicating the SQL runtime/optimization failure.
        """
        if execute:
            return PyDoughSQLException(
                "SQL query execution failed. Please check the query syntax and database connection:\n"
                f"{sql}\nError: {error}"
            )
        else:
            return PyDoughSQLException(
                "SQL query optimization failed. Please check the query syntax:\n"
                f"{sql}\nError: {error}"
            )

    def sql_call_conversion_error(
        self, call: "CallExpression", error: Exception
    ) -> PyDoughException:
        """
        Creates an exception for when the conversion of a call expression from
        Relational to SQL fails.

        Args:
            `call`: The relational function call expression that
            failed to convert.
            `error`: The exception raised during the conversion.

        Returns:
            An exception indicating the SQL call conversion failure.
        """
        return PyDoughQDAGException(
            f"Failed to convert expression {call.to_string(True)} to SQL: {error}"
        )

    def undefined_function_call(
        self, node: "UnqualifiedNode", *args, **kwargs
    ) -> PyDoughException:
        """
        Creates an exception for when a function call is made on an unqualified
        node that is not callable.

        Args:
            `node`: The unqualified node that was called as if it were a
            function.
            `*args`: Positional arguments passed to the call.
            `**kwargs`: Keyword arguments passed to the call.

        Returns:
            An exception indicating that the node is not callable.
        """
        from pydough.unqualified import UnqualifiedAccess, UnqualifiedRoot

        error_message: str = f"PyDough object {node!r} is not callable."
        # If in the form root.XXX, then it is possible that XXXX is a typo of
        # a function name.
        if isinstance(node, UnqualifiedAccess) and isinstance(
            node._parcel[0], UnqualifiedRoot
        ):
            suggestions: list[str] = find_possible_name_matches(
                term_name=node._parcel[1],
                candidates=set(node._parcel[0]._parcel[1]),
                atol=2,
                rtol=0.1,
                min_names=3,
                max_names=5,
                insert_cost=0.5,
                delete_cost=1.0,
                substitution_cost=1.0,
                capital_cost=0.1,
            )

            # Check if there are any suggestions to add
            if len(suggestions) > 0:
                suggestions_str: str = ", ".join(suggestions)
                error_message += f" Did you mean: {suggestions_str}?"
        else:
            error_message += " Did you mean to access an attribute or method?"
        return PyDoughUnqualifiedException(error_message)

    def bad_window_per(
        self,
        per: str,
        ancestral_names: list[str],
        context: "PyDoughCollectionQDAG",
        window: "UnqualifiedWindow",
    ) -> PyDoughException:
        """
        Creates an exception for when the `per` string in a window is malformed.

        Args:
            `per`: The per string that caused the error.
            `ancestral_names`: The names of the ancestors in the context.
            `context`: The collection context where the error occurred.
            `window`: The unqualified window that contains the per string.

        Returns:
            An exception indicating the malformed per string.
        """
        ancestor_name: str
        ancestor_idx: int | None = None
        msg: str | None = None
        components: list[str] = per.split(":")

        # Extract the name/idx components of `per=name:idx`, identifying an
        # error if not in that format.
        if len(components) <= 2:
            if len(components) == 1:
                ancestor_name = components[0]
                ancestor_idx = None
            elif len(components) == 2:
                ancestor_name = components[0]
                if not components[1].isdigit() or int(components[1]) <= 0:
                    msg = "expected the index after ':' to be a positive integer"
                else:
                    ancestor_idx = int(components[1])
            # If an error was not found yet, figure out what is wrong with
            # `name` or `idx`.
            if msg is None:
                if ancestor_name not in ancestral_names:
                    alt_names: list[str] = []
                    for idx, alt_name in enumerate(ancestral_names):
                        if ancestral_names.count(alt_name) == 1:
                            alt_names.append(repr(alt_name))
                        else:
                            alt_idx: int = (
                                ancestral_names[idx + 1 :].count(alt_name) + 1
                            )
                            alt_names.append(repr(f"{alt_name}:{alt_idx}"))
                    msg = f"unrecognized ancestor {ancestor_name!r}; did you mean one of: {', '.join(alt_names)}"
                elif ancestor_idx is None and ancestral_names.count(ancestor_name) > 1:
                    msg = f"per-string {ancestor_name!r} is ambiguous in this context; use the form '{ancestor_name}:index' to disambiguate, where '{ancestor_name}:1' refers to the most recent ancestor"
                else:
                    msg = f"there are not {ancestor_idx} ancestors of the current context with name {ancestor_name!r}"
        else:
            msg = f"expected 0 or 1 ':', found {len(components) - 1})"

        return PyDoughUnqualifiedException(
            f"Error while parsing 'per' string of {window} in context {context} ({msg})"
        )
