"""
Definition of binding infrastructure that maps PyDough operators to
implementations of how to convert them to SQLGlot expressions
"""

__all__ = ["SqlGlotTransformBindings"]

import sqlite3
from collections.abc import Callable, Sequence

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Binary, Case, Concat, Is, Paren, Unary
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

PAREN_EXPRESSIONS = (Binary, Unary, Concat, Is, Case)
"""
The types of SQLGlot expressions that need to be wrapped in parenthesis for the
sake of precedence.
"""


def apply_parens(expression: SQLGlotExpression) -> SQLGlotExpression:
    """
    Determine when due to the next SQL operator not using standard
    function syntax, we may need to apply parentheses to the current
    expression to avoid operator precedence issues.

    Args:
        `expression`: The expression to check and potentially wrap in
        parentheses.

    Returns:
        The expression, wrapped in parentheses if necessary.
    """
    if isinstance(expression, PAREN_EXPRESSIONS):
        return Paren(this=expression)
    else:
        return expression


def convert_sqlite_datetime_extract(format_str: str) -> transform_binding:
    """
    Generate a SQLite-compatible datetime conversion expression for the given
    format string. This is used when a dialect does not support extraction
    functions, so `YEAR(x)` becomes `STRFTIME('%Y', x) :: INT`, etc.

    Args:
        `format_str`: The format string corresponding that should be used
        in the `STRFTIME` call to extract a portion of the date/time of the
        operands.

    Returns:
        A new transform binding that corresponds to an extraction operation
        using the specified `format_str` to do so.
    """

    def impl(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.TimeToStr(
                this=sql_glot_args[0], format=format_str
            ),
            to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
        )

    return impl


def convert_iff_case(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `IFF(a, b, c)` to the expression
    `CASE WHEN a THEN b ELSE c END`, since not every dialect supports IFF.

    Args:
        `raw_args`: The operands to `IFF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `IFF`, after they were
        converted to SQLGlot expressions.

    Returns:
        A `CASE` expression equivalent to the input `IFF` call.
    """
    assert len(sql_glot_args) == 3
    return (
        sqlglot_expressions.Case()
        .when(sql_glot_args[0], sql_glot_args[1])
        .else_(sql_glot_args[2])
    )


def convert_absent(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `ABSENT(X)` to the expression
    `X IS NULL`

    Args:
        `raw_args`: The operands to `ABSENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `ABSENT`, after they were
        converted to SQLGlot expressions.

    Returns:
        The `IS NULL` call corresponding to the `ABSENT` call.
    """
    return sqlglot_expressions.Is(
        this=apply_parens(sql_glot_args[0]), expression=sqlglot_expressions.Null()
    )


def convert_present(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `PRESENT(X)` to the expression
    `X IS NOT NULL`

    Args:
        `raw_args`: The operands to `PRESENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `PRESENT`, after they were
        converted to SQLGlot expressions.

    Returns:
        The `IS NOT NULL` call corresponding to the `PRESENT` call.
    """
    return sqlglot_expressions.Not(
        this=apply_parens(convert_absent(raw_args, sql_glot_args))
    )


def convert_keep_if(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `KEEP_IF(X, Y)` to the expression
    `CASE IF Y THEN X END`.
    Args:
        `raw_args`: The operands to `KEEP_IF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `KEEP_IF`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot case expression equivalent to the `KEEP_IF` call.
    """
    return convert_iff_case(
        None, [sql_glot_args[1], sql_glot_args[0], sqlglot_expressions.Null()]
    )


def convert_monotonic(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `MONOTONIC(A, B, C, ...)` to an
    expression equivalent of `(A <= B) AND (B <= C) AND ...`.

    Args:
        `raw_args`: The operands to `MONOTONIC`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `MONOTONIC`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression equivalent to the `MONOTONIC` call.
    """

    if len(sql_glot_args) < 2:
        return sqlglot_expressions.convert(True)

    exprs: list[SQLGlotExpression] = [apply_parens(expr) for expr in sql_glot_args]
    output_expr: SQLGlotExpression = apply_parens(
        sqlglot_expressions.LTE(this=exprs[0], expression=exprs[1])
    )
    for i in range(2, len(exprs)):
        new_expr: SQLGlotExpression = apply_parens(
            sqlglot_expressions.LTE(this=exprs[i - 1], expression=exprs[i])
        )
        output_expr = sqlglot_expressions.And(this=output_expr, expression=new_expr)
    return output_expr


def convert_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a `CONCAT` expression from a list of arguments.
    This is optimized for the case where all arguments are string literals
    because it impacts the quality of the generated SQL for common cases.

    Args:
        `raw_args`: The operands to `CONCAT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT`, after they were
        converted to SQLGlot expressions.

    Returns:
        A `CONCAT` expression, or equivalent string literal.
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


def convert_slice(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a `SLICE` expression from a list of arguments.

    Args:
        `raw_args`: The operands to `SLICE`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `SLICE`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `SUBSTRING`.
    """
    assert len(sql_glot_args) == 4

    for arg in sql_glot_args[1:]:
        if not isinstance(arg, (sqlglot_expressions.Literal, sqlglot_expressions.Null)):
            raise NotImplementedError(
                "SLICE function currently only supports the slicing arguments being integer literals or absent"
            )

    _, start, stop, step = sql_glot_args
    start_idx: int | None = None
    stop_idx: int | None = None
    if isinstance(start, sqlglot_expressions.Literal):
        if int(start.this) < 0:
            raise NotImplementedError(
                "SLICE function currently only supports non-negative start indices"
            )
        start_idx = int(start.this)
    if isinstance(stop, sqlglot_expressions.Literal):
        if int(stop.this) < 0:
            raise NotImplementedError(
                "SLICE function currently only supports non-negative stop indices"
            )
        stop_idx = int(stop.this)
    if isinstance(step, sqlglot_expressions.Literal):
        if int(step.this) != 1:
            raise NotImplementedError(
                "SLICE function currently only supports a step of 1"
            )

    match (start_idx, stop_idx):
        case (None, None):
            raise sql_glot_args[0]
        case (_, None):
            assert start_idx is not None
            return sqlglot_expressions.Substring(
                this=sql_glot_args[0], start=sqlglot_expressions.convert(start_idx + 1)
            )
        case (None, _):
            assert stop_idx is not None
            return sqlglot_expressions.Substring(
                this=sql_glot_args[0],
                start=sqlglot_expressions.convert(1),
                length=sqlglot_expressions.convert(stop_idx),
            )
        case _:
            assert start_idx is not None
            assert stop_idx is not None
            return sqlglot_expressions.Substring(
                this=sql_glot_args[0],
                start=sqlglot_expressions.convert(start_idx + 1),
                length=sqlglot_expressions.convert(stop_idx - start_idx),
            )


def convert_concat_ws(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a `CONCAT_WS` expression from a list of arguments.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `CONCAT_WS`.
    """
    return sqlglot_expressions.ConcatWs(expressions=sql_glot_args)


def convert_concat_ws_to_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Converts an expression equivalent to a `CONCAT_WS` call into a chain of
    `CONCAT` calls.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `CONCAT_WS`.
    """
    args: list[SQLGlotExpression] = []
    for i in range(1, len(sql_glot_args)):
        if i > 1:
            args.append(sql_glot_args[0])
        args.append(sql_glot_args[i])
    return sqlglot_expressions.Concat(expressions=args)


def convert_like(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a `LIKE` expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        `raw_args`: The operands to `LIKE`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `LIKE`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `LIKE`.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    pattern: SQLGlotExpression = apply_parens(sql_glot_args[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert a `STARTSWITH` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `STARTSWITH`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `STARTSWITH`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `STARTSWITH`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `STARTSWITH`
        by using `LIKE` where the pattern is the original STARTSWITH string,
        prepended with `'%'`.
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
    Convert a `ENDSWITH` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `ENDSWITH`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `ENDSWITH`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `ENDSWITH`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `ENDSWITH`
        by using `LIKE` where the pattern is the original ENDSWITH string,
        prepended with `'%'`.
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
    Convert a `CONTAINS` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `CONTAINS`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `CONTAINS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONTAINS`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `CONTAINS`
        by using `LIKE` where the pattern is the original contains string,
        sandwiched between `'%'` on either side.
    """
    # TODO: (gh #170) update to a different transformation for array/map containment
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
    Convert an `ISIN` call expression to a SQLGlot expression. This is done
    because converting to IN is non-standard.

    Args:
        `raw_args`: The operands to `ISIN`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `ISIN`, after they were converted to
        SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `ISIN`
        by doing `x IN y` on its operands.
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
    Converts a `NDISTINCT` call expression to a SQLGlot expression.

    Args:
        `raw_args`: The operands to `NDISTINCT`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `NDISTINCT`, after they were converted
        to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `NDISTINCT`
        by calling `COUNT(DISTINCT)` on its operand.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    return sqlglot_expressions.Count(
        this=sqlglot_expressions.Distinct(expressions=[column])
    )

def create_convert_time_unit_function(unit: str):
    """
    Creates a function that extracts a specific time unit
    (e.g., HOUR, MINUTE, SECOND) from a SQLGlot expression.

    Args:
        `unit`: The time unit to extract. Must be one of 'HOUR', 'MINUTE',
                or 'SECOND'.
    Returns:
        A function that can convert operands into a SQLGlot expression matching
        the functionality of `EXTRACT(unit FROM expression)`.
    """
    def convert_time_unit(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
        """
        Converts and extracts the specific time unit from a SQLGlot expression.

        Args:
            `raw_args`: The operands passed to the function before they were converted to
            SQLGlot expressions. (Not actively used in this implementation.)
            `sql_glot_args`: The operands passed to the function after they were converted
            to SQLGlot expressions. The first operand is expected to be a timestamp or
                                    datetime.

        Returns:
            The SQLGlot expression matching the functionality of
            `EXTRACT(unit FROM expression)` by extracting the specified time unit
            from the first operand.
        """
        return sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit),
            expression=sql_glot_args[0]
        )

    return convert_time_unit

def convert_sqrt(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
    """
        Support for getting the square root of the operand.

        Args:
            `raw_args`: The operands passed to the function before they were converted to
            SQLGlot expressions. (Not actively used in this implementation.)
            `sql_glot_args`: The operands passed to the function after they were converted
            to SQLGlot expressions.

        Returns:
            The SQLGlot expression matching the functionality of
            `POWER(x,0.5)`,i.e the square root.
    """

    return sqlglot_expressions.Pow(
        this=sql_glot_args[0],
        expression=sqlglot_expressions.Literal.number(0.5)
    )

class SqlGlotTransformBindings:
    """
    Binding infrastructure used to associate PyDough operators with a procedure
    that transforms an invocation of the operator onto certain arguments into a
    SQLGlot expression in a manner that is consistent with the dialect being
    used.
    """

    def __init__(self):
        self.dialect: DatabaseDialect = DatabaseDialect.ANSI
        self.bindings: dict[pydop.PyDoughOperator, transform_binding] = {}
        self.set_dialect(DatabaseDialect.ANSI)

    def set_dialect(self, dialect: DatabaseDialect):
        """
        Switches the dialect used by the function bindings, and changing the
        bindings however necessary.
        """
        self.dialect = dialect
        # Always refresh the default bindings in case the previous dialect
        # overwrote any of them.
        self.add_builtin_bindings()
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
        raw_args: Sequence[RelationalExpression],
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
        """
        Converts an invocation of a PyDough operator into a SQLGlot expression
        in terms of its operands in a manner consistent with the function
        bindings.

        Args:
            `operator`: the PyDough operator corresponding to the function call
            being converted to SQLGlot.
            `raw_args`: the operands to the function, before they were
            converted to SQLGlot expressions.
            `sql_glot_args`: the operands to the function, after they were
            converted to SQLGlot expressions.

        Returns:
            The SQLGlot expression corresponding to the operator invocation on
            the specified operands.
        """
        if operator not in self.bindings:
            # TODO: (gh #169) add support for UDFs
            raise ValueError(f"Unsupported function {operator}")
        binding: transform_binding = self.bindings[operator]
        return binding(raw_args, sql_glot_args)

    def bind_simple_function(
        self, operator: pydop.PyDoughOperator, func: SQLGlotFunction
    ) -> None:
        """
        Adds a function binding for a basic function call.

        Args:
            `operator`: the PyDough operator for the function operation being
            bound.
            `func`: the SQLGlot function for the function it is being bound to.
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
        Adds a function binding for a binary operator.

        Args:
            `operator`: the PyDough operator for the binary operator being
            bound.
            `func`: the SQLGlot function for the binary operator it is being
            bound to.
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
        Adds a function binding for a unary operator.

        Args:
            `operator`: the PyDough operator for the unary operator being
            bound.
            `func`: the SQLGlot function for the unary operator it is being
            bound to.
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
        Adds all of the bindings that are when converting to ANSI SQL, or are
        standard across dialects.
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
        self.bindings[pydop.SLICE] = convert_slice
        self.bindings[pydop.JOIN_STRINGS] = convert_concat_ws

        # Numeric functions
        self.bind_simple_function(pydop.ABS, sqlglot_expressions.Abs)
        self.bind_simple_function(pydop.ROUND, sqlglot_expressions.Round)

        # Conditional functions
        self.bind_simple_function(pydop.DEFAULT_TO, sqlglot_expressions.Coalesce)
        self.bindings[pydop.IFF] = convert_iff_case
        self.bindings[pydop.ISIN] = convert_isin
        self.bindings[pydop.PRESENT] = convert_present
        self.bindings[pydop.ABSENT] = convert_absent
        self.bindings[pydop.KEEP_IF] = convert_keep_if
        self.bindings[pydop.MONOTONIC] = convert_monotonic

        # Datetime functions
        self.bind_unop(pydop.YEAR, sqlglot_expressions.Year)
        self.bind_unop(pydop.MONTH, sqlglot_expressions.Month)
        self.bind_unop(pydop.DAY, sqlglot_expressions.Day)
        self.bindings[pydop.HOUR] = create_convert_time_unit_function("HOUR")
        self.bindings[pydop.MINUTE] = create_convert_time_unit_function("MINUTE")
        self.bindings[pydop.SECOND] = create_convert_time_unit_function("SECOND")

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
        self.bind_binop(pydop.POW,sqlglot_expressions.Pow)
        self.bind_binop(pydop.POWER,sqlglot_expressions.Pow)
        self.bindings[pydop.SQRT] = convert_sqrt

        # Unary operators
        self.bind_unop(pydop.NOT, sqlglot_expressions.Not)

    def add_sqlite_bindings(self) -> None:
        """
        Adds the bindings & overrides that are specific to SQLite.
        """
        # Use IF function instead of CASE if the SQLite version is recent
        # enough.
        if sqlite3.sqlite_version >= "3.32":
            self.bind_simple_function(pydop.IFF, sqlglot_expressions.If)

        # Datetime function overrides
        self.bindings[pydop.YEAR] = convert_sqlite_datetime_extract("'%Y'")
        self.bindings[pydop.MONTH] = convert_sqlite_datetime_extract("'%m'")
        self.bindings[pydop.DAY] = convert_sqlite_datetime_extract("'%d'")
        self.bindings[pydop.HOUR] = convert_sqlite_datetime_extract("'%H'")
        self.bindings[pydop.MINUTE] = convert_sqlite_datetime_extract("'%M'")
        self.bindings[pydop.SECOND] = convert_sqlite_datetime_extract("'%S'")

        # String function overrides
        if sqlite3.sqlite_version < "3.44.1":
            self.bindings[pydop.JOIN_STRINGS] = convert_concat_ws_to_concat
