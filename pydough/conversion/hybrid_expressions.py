"""
Definitions of the hybrid expression abstract base class and its concrete
subclasses. These represent expressions within each hybrid operation, such
as columns or function calls.
"""

__all__ = [
    "HybridBackRefExpr",
    "HybridChildRefExpr",
    "HybridCollation",
    "HybridColumnExpr",
    "HybridCorrelExpr",
    "HybridExpr",
    "HybridFunctionExpr",
    "HybridLiteralExpr",
    "HybridRefExpr",
]

import copy
from abc import ABC, abstractmethod

import pydough.pydough_operators as pydop
from pydough.qdag import (
    ColumnProperty,
    Literal,
)
from pydough.types import PyDoughType


class HybridExpr(ABC):
    """
    The base class for expression nodes within a hybrid operation.
    """

    def __init__(self, typ: PyDoughType):
        self.typ: PyDoughType = typ

    def __eq__(self, other):
        return type(self) is type(other) and repr(self) == repr(other)

    def __hash__(self):
        return hash(repr(self))

    def __repr__(self):
        return self.to_string()

    @abstractmethod
    def to_string(self) -> str:
        """
        Returns a string representation of the expression.
        """

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        """
        Renames references in an expression if contained in a renaming
        dictionary.

        Args:
            `renamings`: a dictionary mapping names of any references to the
            new name that they should adopt.

        Returns:
            The transformed copy of self, if necessary, otherwise
            just returns self.
        """
        return self

    def shift_back(self, levels: int, shift_correl: bool = True) -> "HybridExpr":
        """
        Promotes a HybridRefExpr into a HybridBackRefExpr with the specified
        number of levels, or increases the number of levels of a
        HybridBackRefExpr by the specified number of levels.

        Args:
            `levels`: the amount of back levels to increase by.
            `shift_correl`: whether to shift correlated references
            back as well. If False, leaves their contents alone.

        Returns:
            The transformed HybridBackRefExpr.
        """
        return self

    def replace_expressions(
        self,
        replacements: dict["HybridExpr", "HybridExpr"],
    ) -> "HybridExpr":
        """
        TODO
        """
        if self in replacements:
            return copy.deepcopy(replacements[self])
        return self

    def squish_backrefs_into_correl(self, level_threshold: int | None) -> "HybridExpr":
        """
        TODO
        """
        return self

    def count_correlated_levels(self) -> int:
        """
        TODO
        """
        return 0

    def contains_correlates(self) -> bool:
        """
        Returns whether this expression contains any correlated references.
        """
        return False

    def get_correlate_names(self, levels: int) -> set[str]:
        """
        Returns the set of names of variables that are correlated a certain
        number of levels within the expression.
        """
        return set()

    def has_correlated_window_function(self, levels: int) -> bool:
        """
        Returns whether this expression contains any window functions
        with correlates with at least a certain number of levels.
        """
        return False

    def contains_window_functions(self) -> bool:
        """
        Returns whether this expression contains any window functions.
        """
        return False


class HybridCollation:
    """
    Class for HybridExpr terms that are another HybridExpr term wrapped in
    information about how to sort by them.
    """

    def __init__(self, expr: "HybridExpr", asc: bool, na_first: bool):
        self.expr: HybridExpr = expr
        self.asc: bool = asc
        self.na_first: bool = na_first

    def __repr__(self):
        suffix: str = (
            f"{'asc' if self.asc else 'desc'}_{'first' if self.na_first else 'last'}"
        )
        return f"({self.expr!r}):{suffix}"


def all_same(exprs: list[HybridExpr], renamed_exprs: list[HybridExpr]) -> bool:
    """
    Returns whether two lists of hybrid expressions are identical, down to
    identity.
    """
    return len(exprs) == len(renamed_exprs) and all(
        expr is renamed_expr for expr, renamed_expr in zip(exprs, renamed_exprs)
    )


class HybridColumnExpr(HybridExpr):
    """
    Class for HybridExpr terms that are references to a column from a table.
    """

    def __init__(self, column: ColumnProperty):
        super().__init__(column.pydough_type)
        self.column: ColumnProperty = column

    def to_string(self):
        return repr(self.column)

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        return HybridBackRefExpr(self.column.column_property.name, levels, self.typ)


class HybridRefExpr(HybridExpr):
    """
    Class for HybridExpr terms that are references to a term from a preceding
    HybridOperation.
    """

    def __init__(self, name: str, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name

    def to_string(self):
        return self.name

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        if self.name in renamings:
            return HybridRefExpr(renamings[self.name], self.typ)
        return self

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        if levels == 0:
            return self
        return HybridBackRefExpr(self.name, levels, self.typ)


class HybridChildRefExpr(HybridExpr):
    """
    Class for HybridExpr terms that are references to a term from a child
    operation.
    """

    def __init__(self, name: str, child_idx: int, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name
        self.child_idx: int = child_idx

    def to_string(self):
        return f"${self.child_idx}.{self.name}"


class HybridBackRefExpr(HybridExpr):
    """
    Class for HybridExpr terms that are references to a term from an
    ancestor operation.
    """

    def __init__(self, name: str, back_idx: int, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name
        self.back_idx: int = back_idx

    def to_string(self):
        return f"BACK({self.back_idx}).{self.name}"

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        return HybridBackRefExpr(self.name, self.back_idx + levels, self.typ)

    def squish_backrefs_into_correl(self, level_threshold: int | None):
        if level_threshold is not None and self.back_idx >= level_threshold:
            levels_remaining = self.back_idx - level_threshold
            parent_expr: HybridExpr
            if levels_remaining == 0:
                parent_expr = HybridRefExpr(self.name, self.typ)
            else:
                parent_expr = HybridBackRefExpr(self.name, levels_remaining, self.typ)
            return HybridCorrelExpr(parent_expr)
        else:
            return self


class HybridSidedRefExpr(HybridExpr):
    """
    Class for HybridExpr terms inside of a general join condition that point to
    the parent side of the join (similar to a correlated reference).
    """

    def __init__(self, name: str, typ: PyDoughType):
        super().__init__(typ)
        self.name: str = name

    def to_string(self):
        return f"PARENT.{self.name}"


class HybridCorrelExpr(HybridExpr):
    """
    Class for HybridExpr terms that are expressions from a parent hybrid tree
    rather than an ancestor, which requires a correlated reference.
    """

    def __init__(self, expr: HybridExpr):
        super().__init__(expr.typ)
        self.expr: HybridExpr = expr

    def to_string(self):
        return f"CORREL({self.expr})"

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        if shift_correl and levels > 0:
            return HybridCorrelExpr(self.expr.shift_back(levels))
        return self

    def contains_correlates(self) -> bool:
        return True

    def count_correlated_levels(self) -> int:
        return 1 + self.expr.count_correlated_levels()

    def get_correlate_names(self, levels: int) -> set[str]:
        result: set[str] = set()
        expr: HybridExpr = self
        for _ in range(levels):
            if isinstance(expr, HybridCorrelExpr):
                expr = expr.expr
            else:
                return result
        if isinstance(expr, HybridRefExpr):
            result.add(expr.name)
        return result

    def squish_backrefs_into_correl(self, level_threshold: int | None):
        return HybridCorrelExpr(self)


class HybridLiteralExpr(HybridExpr):
    """
    Class for HybridExpr terms that are literals.
    """

    def __init__(self, literal: Literal):
        super().__init__(literal.pydough_type)
        self.literal: Literal = literal

    def to_string(self):
        return repr(self.literal)


class HybridFunctionExpr(HybridExpr):
    """
    Class for HybridExpr terms that are function calls.
    """

    def __init__(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[HybridExpr],
        typ: PyDoughType,
    ):
        super().__init__(typ)
        self.operator: pydop.PyDoughExpressionOperator = operator
        self.args: list[HybridExpr] = args

    def to_string(self):
        arg_strings: list[str] = [
            f"({arg!r})"
            if isinstance(self.operator, pydop.BinaryOperator)
            and isinstance(arg, HybridFunctionExpr)
            and isinstance(arg.operator, pydop.BinaryOperator)
            else repr(arg)
            for arg in self.args
        ]
        return self.operator.to_string(arg_strings)

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        renamed_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.args
        ]
        if all_same(self.args, renamed_args):
            return self
        return HybridFunctionExpr(self.operator, renamed_args, self.typ)

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        # Shift all of the inputs to the function. Return None if any of them
        # cannot be shifted.
        shifted_args: list[HybridExpr] = []
        for arg in self.args:
            shifted_args.append(arg.shift_back(levels))
        return HybridFunctionExpr(self.operator, shifted_args, self.typ)

    def replace_expressions(
        self,
        replacements: dict[HybridExpr, HybridExpr],
    ) -> HybridExpr:
        """
        TODO
        """
        if self in replacements:
            return replacements[self]
        for idx, arg in enumerate(self.args):
            self.args[idx] = arg.replace_expressions(replacements)
        return self

    def squish_backrefs_into_correl(self, level_threshold: int | None):
        """
        TODO
        """
        for idx, arg in enumerate(self.args):
            self.args[idx] = arg.squish_backrefs_into_correl(level_threshold)
        return self

    def contains_correlates(self) -> bool:
        return any(arg.contains_correlates() for arg in self.args)

    def contains_window_functions(self) -> bool:
        return any(arg.contains_window_functions() for arg in self.args)

    def count_correlated_levels(self) -> int:
        correl_levels: int = 0
        for arg in self.args:
            correl_levels = max(correl_levels, arg.count_correlated_levels())
        return correl_levels

    def get_correlate_names(self, levels: int) -> set[str]:
        result: set[str] = set()
        for arg in self.args:
            result.update(arg.get_correlate_names(levels))
        return result

    def has_correlated_window_function(self, levels: int) -> bool:
        return any(arg.has_correlated_window_function(levels) for arg in self.args)


class HybridWindowExpr(HybridExpr):
    """
    Class for HybridExpr terms that are window function calls.
    """

    def __init__(
        self,
        window_func: pydop.ExpressionWindowOperator,
        args: list[HybridExpr],
        partition_args: list[HybridExpr],
        order_args: list[HybridCollation],
        typ: PyDoughType,
        kwargs: dict[str, object],
    ):
        super().__init__(typ)
        self.window_func: pydop.ExpressionWindowOperator = window_func
        self.args: list[HybridExpr] = args
        self.partition_args: list[HybridExpr] = partition_args
        self.order_args: list[HybridCollation] = order_args
        self.kwargs: dict[str, object] = kwargs

    def to_string(self):
        args_str = ""
        args_str += f"by=[{', '.join([str(arg) for arg in self.args])}]"
        args_str += (
            f", partition=[{', '.join([str(arg) for arg in self.partition_args])}]"
        )
        args_str += f", order=[{', '.join([str(arg) for arg in self.order_args])}]"
        if "allow_ties" in self.kwargs:
            args_str += f", allow_ties={self.kwargs['allow_ties']}"
            if "dense" in self.kwargs:
                args_str += f", dense={self.kwargs['dense']}"
        return f"{self.window_func.function_name}({args_str})"

    def apply_renamings(self, renamings: dict[str, str]) -> "HybridExpr":
        renamed_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.args
        ]
        renamed_partition_args: list[HybridExpr] = [
            arg.apply_renamings(renamings) for arg in self.partition_args
        ]
        renamed_order_args: list[HybridCollation] = []
        for col_arg in self.order_args:
            collation_expr: HybridExpr = col_arg.expr
            renamed_expr: HybridExpr = collation_expr.apply_renamings(renamings)
            if renamed_expr is collation_expr:
                renamed_order_args.append(col_arg)
            else:
                renamed_order_args.append(
                    HybridCollation(renamed_expr, col_arg.asc, col_arg.na_first)
                )
        if (
            all_same(self.args, renamed_args)
            and all_same(self.partition_args, renamed_partition_args)
            and all_same(
                [arg.expr for arg in self.order_args],
                [arg.expr for arg in renamed_order_args],
            )
        ):
            return self
        return HybridWindowExpr(
            self.window_func,
            renamed_args,
            renamed_partition_args,
            renamed_order_args,
            self.typ,
            self.kwargs,
        )

    def shift_back(self, levels: int, shift_correl: bool = True) -> HybridExpr:
        # Shift all of the inputs to the window function (including regular,
        # partition, and order inputs). Return None if any of them cannot
        # be shifted.
        shifted_args: list[HybridExpr] = []
        shifted_partition_args: list[HybridExpr] = []
        shifted_order_args: list[HybridCollation] = []
        for arg in self.args:
            shifted_args.append(arg.shift_back(levels))
        for arg in self.partition_args:
            shifted_partition_args.append(arg.shift_back(levels))
        for order_arg in self.order_args:
            shifted_order_args.append(
                HybridCollation(
                    order_arg.expr.shift_back(levels), order_arg.asc, order_arg.na_first
                )
            )
        return HybridWindowExpr(
            self.window_func,
            shifted_args,
            shifted_partition_args,
            shifted_order_args,
            self.typ,
            self.kwargs,
        )

    def replace_expressions(
        self,
        replacements: dict[HybridExpr, HybridExpr],
    ) -> HybridExpr:
        if self in replacements:
            return replacements[self]
        for idx, arg in enumerate(self.args):
            self.args[idx] = arg.replace_expressions(replacements)
        for idx, part_arg in enumerate(self.partition_args):
            self.partition_args[idx] = part_arg.replace_expressions(replacements)
        for idx, order_arg in enumerate(self.order_args):
            self.order_args[idx].expr = order_arg.expr.replace_expressions(replacements)
        return self

    def squish_backrefs_into_correl(self, level_threshold: int | None):
        """
        TODO
        """
        for idx, arg in enumerate(self.args):
            self.args[idx] = arg.squish_backrefs_into_correl(level_threshold)
        for idx, part_arg in enumerate(self.partition_args):
            self.partition_args[idx] = part_arg.squish_backrefs_into_correl(
                level_threshold
            )
        for idx, order_arg in enumerate(self.order_args):
            self.order_args[idx].expr = order_arg.expr.squish_backrefs_into_correl(
                level_threshold
            )
        return self

    def count_correlated_levels(self) -> int:
        correl_levels: int = 0
        for arg in self.args:
            correl_levels = max(correl_levels, arg.count_correlated_levels())
        for part_arg in self.partition_args:
            correl_levels = max(correl_levels, part_arg.count_correlated_levels())
        for order_arg in self.order_args:
            correl_levels = max(correl_levels, order_arg.expr.count_correlated_levels())
        return correl_levels

    def contains_correlates(self) -> bool:
        return (
            any(arg.contains_correlates() for arg in self.args)
            or any(
                partition_arg.contains_correlates()
                for partition_arg in self.partition_args
            )
            or any(
                order_arg.expr.contains_correlates() for order_arg in self.order_args
            )
        )

    def get_correlate_names(self, levels: int) -> set[str]:
        result: set[str] = set()
        for arg in self.args:
            result.update(arg.get_correlate_names(levels))
        for partition_arg in self.partition_args:
            result.update(partition_arg.get_correlate_names(levels))
        for order_arg in self.order_args:
            result.update(order_arg.expr.get_correlate_names(levels))
        return result

    def contains_window_functions(self) -> bool:
        return True

    def has_correlated_window_function(self, levels: int) -> bool:
        if self.count_correlated_levels() >= levels:
            return True
        return (
            any(arg.has_correlated_window_function(levels) for arg in self.args)
            or any(
                partition_arg.has_correlated_window_function(levels)
                for partition_arg in self.partition_args
            )
            or any(
                order_arg.expr.has_correlated_window_function(levels)
                for order_arg in self.order_args
            )
        )
