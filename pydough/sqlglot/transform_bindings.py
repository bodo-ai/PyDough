"""
Definition of binding infrastructure that maps PyDough operators to
implementations of how to convert them to SQLGlot expressions
"""

__all__ = ["SqlGlotTransformBindings"]

import sqlite3
from collections.abc import Callable, Sequence

import sqlglot.expressions as sqlglot_expressions
from sqlglot.dialects import Dialect
from sqlglot.expressions import Binary, Concat, Paren
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Func as SQLGlotFunction

import pydough.pydough_operators as pydop
from pydough.database_connectors.database_connector import DatabaseDialect
from pydough.relational.relational_expressions import RelationalExpression

operator = pydop.PyDoughOperator
transform_binding = Callable[
    [Sequence[RelationalExpression] | None, Sequence[SQLGlotExpression]],
    SQLGlotExpression,
]


def apply_parens(expression: SQLGlotExpression) -> SQLGlotExpression:
    """
    Determine when due to the next SQL operator not using standard
    function syntax, we may need to apply parentheses to the current
    expression to avoid operator precedence issues.

    Args:
        expression (SQLGlotExpression): The expression to check.

    Returns:
        SQLGlotExpression: The expression with parentheses applied if
            necessary.
    """
    if isinstance(expression, (Binary, Concat)):
        return Paren(this=expression)
    else:
        return expression


def _convert_sqlite_datetime(
    column: SQLGlotExpression, format_str: str
) -> SQLGlotExpression:
    """
    Generate a SQLite-compatible datetime conversion expression for the given
    format string.

    Args:
        column (SQLGlotExpression): The column to convert.
        format_str (str): The format string.
    """
    return sqlglot_expressions.Cast(
        this=sqlglot_expressions.TimeToStr(this=column, format=format_str),
        to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
    )


def convert_year(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a YEAR call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert YEAR
    to equivalent SQL operation in SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of year.
    """
    return _convert_sqlite_datetime(sql_glot_args[0], "'%Y'")


def convert_month(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a MONTH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert MONTH
    to equivalent SQL operation in SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of month.
    """
    return _convert_sqlite_datetime(sql_glot_args[0], "'%m'")


def convert_day(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a DAY call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert DAY
    to equivalent SQL operation in SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of day.
    """
    return _convert_sqlite_datetime(sql_glot_args[0], "'%d'")


def convert_iff_case(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    TODO: add function docstring
    """
    assert len(sql_glot_args) == 3
    return (
        sqlglot_expressions.Case()
        .when(sql_glot_args[0], sql_glot_args[1])
        .else_(sql_glot_args[2])
    )


def convert_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a CONCAT expression from a list of arguments.
    This is optimized for the case where all arguments are string literals
    because it impacts the quality of the generated SQL for common cases.

    Args:
        sql_glot_args: The list of arguments to concatenate.

    Returns:
        SQLGlotExpression: A CONCAT expression or equivalent string literal.
    """
    # Fast path for all arguments as string literals.
    if all(
        isinstance(arg, sqlglot_expressions.Literal) and arg.is_string
        for arg in sql_glot_args
    ):
        return sqlglot_expressions.convert("".join(arg.this for arg in sql_glot_args))
    else:
        inputs: list[SQLGlotExpression] = [apply_parens(arg) for arg in sql_glot_args]
        return Concat(expressions=inputs)


def convert_like(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a LIKE expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of like.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    pattern: SQLGlotExpression = apply_parens(sql_glot_args[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a STARTSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert STARTSWITH
    to a LIKE expression for SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of startswith.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sql_glot_args[1], sqlglot_expressions.convert("%")],
    )
    return convert_like(None, [column, pattern])


def convert_endswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a ENDSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert ENDSWITH
    to a LIKE expression for SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of endswith.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sqlglot_expressions.convert("%"), sql_glot_args[1]],
    )
    return convert_like(None, [column, pattern])


def convert_contains(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a CONTAINS call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert CONTAINS
    to a LIKE expression for SQLite.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of contains.
    """
    # TODO: Update when contains maps to multiple functions (e.g. ARRAY_CONTAINS).
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [
            sqlglot_expressions.convert("%"),
            sql_glot_args[1],
            sqlglot_expressions.convert("%"),
        ],
    )
    return convert_like(None, [column, pattern])


def convert_isin(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a ISIN call expression to a SQLGlot expression. This
    is done because converting to IN is non-standard.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of isin.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    # Note: We only handle the case with multiple literals where all
    # literals are in the same literal expression. This code will need
    # to change when we support PyDough expressions like:
    # Collection.WHERE(ISIN(name, plural_subcollection.name))
    values: SQLGlotExpression = sql_glot_args[1]
    return sqlglot_expressions.In(this=column, expressions=values)


def convert_ndistinct(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a NDISTINCT call expression to a SQLGlot expression.

    Args:
        sql_glot_args: The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of NDISTINCT.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    return sqlglot_expressions.Count(
        this=sqlglot_expressions.Distinct(expressions=[column])
    )


class SqlGlotTransformBindings:
    """
    TODO: add class docstring
    """

    def __init__(self, dialect: DatabaseDialect):
        self.dialect: Dialect = dialect
        self.bindings: dict[pydop.PyDoughOperator, transform_binding] = {}
        self.add_builtin_bindings()
        self.set_dialect(dialect)

    def set_dialect(self, dialect: Dialect):
        """
        TODO: add function docstring
        """
        self.dialect = dialect
        match dialect:
            case DatabaseDialect.ANSI:
                pass
            case DatabaseDialect.SQLITE:
                self.add_sqlite_bindings()
            case _:
                raise Exception(
                    f"TODO: support dialect {dialect} in SQLGlot transformation"
                )

    def call(
        self,
        operator: pydop.PyDoughOperator,
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
        """
        TODO: add function docstring
        """
        if operator not in self.bindings:
            # TODO: add support for UDFs
            raise ValueError(f"Unsupported function {operator}")
        binding: transform_binding = self.bindings[operator]
        return binding(raw_args, sql_glot_args)

    def bind_simple_function(
        self, operator: pydop.PyDoughOperator, func: SQLGlotFunction
    ) -> None:
        """
        TODO: add function docstring
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
        ) -> SQLGlotExpression:
            return func.from_arg_list(sql_glot_args)

        self.bindings[operator] = impl

    def bind_binop(
        self, operator: pydop.PyDoughOperator, func: SQLGlotFunction
    ) -> None:
        """
        TODO: add function docstring
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
        ) -> SQLGlotExpression:
            assert len(sql_glot_args) >= 2
            # Note: SQLGlot explicit inserts parentheses for binary operations
            # during parsing.
            output_expr = apply_parens(sql_glot_args[0])
            for expr in sql_glot_args[1:]:
                other_expr: SQLGlotExpression = apply_parens(expr)
                # Build the expressions on the left since the operator is left-associative.
                output_expr = func(this=output_expr, expression=other_expr)
            return output_expr

        self.bindings[operator] = impl

    def bind_unop(self, operator: pydop.PyDoughOperator, func: SQLGlotFunction) -> None:
        """
        TODO: add function docstring
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
        ) -> SQLGlotExpression:
            assert len(sql_glot_args) == 1
            return func(this=sql_glot_args[0])

        self.bindings[operator] = impl

    def add_builtin_bindings(self) -> None:
        """
        TODO: add function docstring
        """
        # Aggregation functions
        self.bind_simple_function(pydop.SUM, sqlglot_expressions.Sum)
        self.bind_simple_function(pydop.AVG, sqlglot_expressions.Avg)
        self.bind_simple_function(pydop.COUNT, sqlglot_expressions.Count)
        self.bind_simple_function(pydop.MIN, sqlglot_expressions.Min)
        self.bind_simple_function(pydop.MAX, sqlglot_expressions.Max)
        self.bindings[pydop.NDISTINCT] = convert_ndistinct

        # String functions
        self.bind_simple_function(pydop.LOWER, sqlglot_expressions.Lower)
        self.bind_simple_function(pydop.UPPER, sqlglot_expressions.Upper)
        self.bind_simple_function(pydop.LENGTH, sqlglot_expressions.Length)
        self.bindings[pydop.STARTSWITH] = convert_startswith
        self.bindings[pydop.ENDSWITH] = convert_endswith
        self.bindings[pydop.CONTAINS] = convert_contains
        self.bindings[pydop.LIKE] = convert_like

        # Numeric functions
        self.bind_simple_function(pydop.ABS, sqlglot_expressions.Abs)

        # Conditional functions
        self.bind_simple_function(pydop.DEFAULT_TO, sqlglot_expressions.Coalesce)
        self.bindings[pydop.ISIN] = convert_isin
        self.bindings[pydop.IFF] = convert_iff_case

        # Datetime functions
        self.bind_unop(pydop.YEAR, sqlglot_expressions.Year)
        self.bind_unop(pydop.MONTH, sqlglot_expressions.Month)
        self.bind_unop(pydop.DAY, sqlglot_expressions.Day)

        # Binary operators
        self.bind_binop(pydop.ADD, sqlglot_expressions.Add)
        self.bind_binop(pydop.SUB, sqlglot_expressions.Sub)
        self.bind_binop(pydop.MUL, sqlglot_expressions.Mul)
        self.bind_binop(pydop.DIV, sqlglot_expressions.Div)
        self.bind_binop(pydop.EQU, sqlglot_expressions.EQ)
        self.bind_binop(pydop.GEQ, sqlglot_expressions.GTE)
        self.bind_binop(pydop.GRT, sqlglot_expressions.GT)
        self.bind_binop(pydop.LEQ, sqlglot_expressions.LTE)
        self.bind_binop(pydop.LET, sqlglot_expressions.LT)
        self.bind_binop(pydop.NEQ, sqlglot_expressions.NEQ)
        self.bind_binop(pydop.BAN, sqlglot_expressions.And)
        self.bind_binop(pydop.BOR, sqlglot_expressions.Or)

        # Unary operators
        self.bind_unop(pydop.NOT, sqlglot_expressions.Not)

    def add_sqlite_bindings(self) -> None:
        """
        TODO: add function docstring
        """
        # Use IF function instead of CASE if the SQLite version is recent
        # enough.
        if sqlite3.sqlite_version >= "3.32":
            self.bind_simple_function(pydop.IFF, sqlglot_expressions.If)

        # Datetime function overrides
        self.bindings[pydop.YEAR] = convert_year
        self.bindings[pydop.MONTH] = convert_month
        self.bindings[pydop.DAY] = convert_day
