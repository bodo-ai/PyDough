"""
Definition of PyDough operator class for functions that return an expression
with branching logic based on keyword arguments.
"""

__all__ = ["KeywordBranchingExpressionFunctionOperator"]

from typing import Any

from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)

from .expression_function_operators import ExpressionFunctionOperator


class KeywordBranchingExpressionFunctionOperator(ExpressionFunctionOperator):
    """
    Implementation class for PyDough operators that return an expression,
    represent a function call, and can branch to different implementations
    based on keyword arguments.

    This allows multiple operators to share the same name but be distinguished
    by keyword arguments.
    """

    def __init__(
        self,
        function_name: str,
        is_aggregation: bool,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
        kwarg_defaults: dict[str, Any] | None = None,
    ):
        super().__init__(function_name, is_aggregation, verifier, deducer)
        self._kwarg_defaults: dict[str, Any] = kwarg_defaults or {}
        self._implementations: list[
            tuple[ExpressionFunctionOperator, dict[str, Any]]
        ] = []

    @property
    def kwarg_defaults(self) -> dict[str, Any]:
        """
        Default values for keyword arguments
        """
        return self._kwarg_defaults

    @property
    def implementations(
        self,
    ) -> list[tuple[ExpressionFunctionOperator, dict[str, Any]]]:
        """
        List of implementing operators with specific keyword arguments
        """
        return self._implementations

    def with_kwarg(
        self, func_str: str, kwargs: dict[str, Any]
    ) -> ExpressionFunctionOperator:
        """
        Creates a new implementing operator with specific keyword values
        and registers it with this branching operator.

        Args:
            `func_str`: The name of the function to implement
            `kwargs`: The keyword argument values that identify this implementation

        Returns:
            A new operator that is registered with this branching operator
        """
        impl: ExpressionFunctionOperator = ExpressionFunctionOperator(
            func_str,
            self.is_aggregation,
            self.verifier,
            self.deducer,
            public=False,  # This operator is not public
        )
        # Store the kwargs that identify this implementation
        final_kwargs: dict[str, Any] = self.kwarg_defaults.copy()
        final_kwargs.update(kwargs)
        expr_func_op_with_kwargs: tuple[ExpressionFunctionOperator, dict[str, Any]] = (
            impl,
            final_kwargs,
        )

        # Add to implementations list
        self._implementations.append(expr_func_op_with_kwargs)

        return impl

    def _get_suffix_from_kwargs(self, kwargs: dict[str, Any]) -> str:
        """
        Generate a name suffix from kwargs for the implementing operator
        """
        parts = []
        for key, value in sorted(kwargs.items()):
            parts.append(f"{key}_{value}")
        return "_".join(parts)

    def find_matching_implementation(
        self, kwargs: dict[str, Any]
    ) -> ExpressionFunctionOperator | None:
        """
        Find an implementing operator that matches the provided kwargs

        Args:
            `kwargs`: The keyword arguments to match against

        Returns:
            The matching operator or None if no match is found
        """
        # Apply defaults for any missing kwargs
        effective_kwargs = self.kwarg_defaults.copy()
        effective_kwargs.update(kwargs)
        # Look for a matching implementation
        for impl, impl_kwargs in self.implementations:
            # Check if both dictionaries have the same key-value pairs
            if (
                impl_kwargs
                and impl_kwargs.keys() == effective_kwargs.keys()
                and all(impl_kwargs[k] == effective_kwargs[k] for k in impl_kwargs)
            ):
                return impl

        return None
