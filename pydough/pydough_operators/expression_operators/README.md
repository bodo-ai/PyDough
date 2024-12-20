# PyDough Expression Operators

This subdirectory of the PyDough operators directory deals with operators that return expressions. These operators are a subclass of operators that return an expression (as opposed to a collection).

The expression_operators module provides functionality to define and manage various operators that can be used within PyDough expressions.

## Available APIs

### [expression_operator.py](expression_operator.py)

- `PyDoughExpressionOperator`: The base class for PyDough operators that return an expression. In addition to having a verifier, all such classes have a deducer to infer the type of the returned expression.
 - `verifier`: The type verification function used by the operator.
 - `deducer`: The return type inference function used by the operator.
 - `function_name`: The name of the function that this operator represents.
 - `requires_enclosing_parens`: Identifies whether an invocation of an operator converted to a string must be wrapped in parentheses before being inserted into its parent's string representation.
 - `infer_return_type`: Returns the expected PyDough type of the operator when called on the provided arguments.
 - `to_string`: Returns the string representation of the operator when called on its arguments.

### [expression_function_operators.py](expression_function_operators.py)

- `ExpressionFunctionOperator`: Implementation class for PyDough operators that return an expression and represent a function call, such as `LOWER` or `SUM`.

### [binary_operators.py](binary_operators.py)

- `BinOp`: Enum class used to describe the various binary operations.
- `BinaryOperator`: Implementation class for PyDough operators that return an expression and represent a binary operation, such as addition.

### [registered_expression_operators.py](registered_expression_operators.py)

Definition bindings of built-in PyDough operators that return an expression. The operations currently defined in the builtins are shown below.

#### Binary Operators

These are created with an infix operator syntax instead of called as a function.

- `ADD` (`+`): binary operator for addition.
- `SUB` (`-`): binary operator for subtraction.
- `MUL` (`*`): binary operator for multiplication.
- `DIV` (`/`): binary operator for division.
- `POW` (`**`): binary operator for exponentiation.
- `MOD` (`%`): binary operator for modulo.
- `LET` (`<`): binary operator for less-than.
- `LEQ` (`<=`): binary operator for less-than-or-equal.
- `GRT` (`>`): binary operator for greater-than.
- `GEQ` (`>=`): binary operator for greater-than-or-equal.
- `EQU` (`==`): binary operator for equal.
- `NEQ` (`!=`): binary operator for not-equal.
- `BAN` (`&`): binary operator for a logical AND.
- `BOR` (`|`): binary operator for a logical OR.
- `BXR` (`^`): binary operator for a logical XOR.

#### Unary Operators

These are created with a prefix operator syntax instead of called as a function.

- `NOT` (`~`): unary operator for a logical NOT.

#### Scalar Functions

These functions must be called on singular data as a function.

##### String Functions

- `LOWER`: converts a string to lowercase.
- `UPPER`: converts a string to uppercase.
- `LENGTH`: returns the length of a string.
- `STARTSWITH`: returns whether the first argument string starts with the second argument string.
- `ENDSWITH`: returns whether the first argument string ends with the second argument string.
- `CONTAINS`: returns whether the first argument string contains the second argument string.
- `LIKES`: returns whether the first argument matches the SQL pattern text of the second argument, where `_` is a 1 character wildcard and `%` is an 0+ character wildcard.

##### Datetime Functions

- `YEAR`: returns the year component of a datetime.
- `MONTH`: returns the month component of a datetime.
- `DAY`: returns the day component of a datetime.

##### Conditional Functions

- `IFF`: if the first argument is true returns the second argument, otherwise returns the third argument.
- `DEFAULT_TO`: returns the first of its arguments that is non-null.

##### Numeric Functions

- `ABS`: returns the absolute value of the input.

#### Aggregation Functions

These functions can be called on plural data to aggregate it into a singular expression.

##### Simple Aggregations

- `SUM`: returns the result of adding all of the values of a plural expression.
- `AVG`: returns the result of taking the average of the values of a plural expression.
- `MIN`: returns the largest out of the values of a plural expression.
- `MAX`: returns the smallest out of the values of a plural expression.
- `COUNT`: counts how many non-null values exist in a plural expression (special: see collection aggregations).
- `NDISTINCT`: counts how many unique values exist in a plural expression (special: see collection aggregations).

##### Collection Aggregations

- `COUNT`: if called on a subcollection, returns how many records of it exist for each record of the current collection (if called on an expression instead of collection, see simple aggregations).
- `NDISTINCT`: if called on a subcollection, returns how many distinct records of it exist for each record of the current collection (if called on an expression instead of collection, see simple aggregations).
- `HAS`: called on a subcollection and returns whether any records of the subcollection for each record of the current collection. Equivalent to `COUNT(X) > 0`.
- `HASNOT`: called on a subcollection and returns whether there are no records of the subcollection for each record of the current collection. Equivalent to `COUNT(X) == 0`.

## Interaction with Type Inference

Expression operators interact with the type inference module to ensure that the arguments passed to them are valid and to infer the return types of those expressions. This helps maintain type safety and correctness in PyDough operations. Every operator has a type verifier object and a type deducer object.

The type verifier is invoked whenever the operator is used in a function call expression with QDAG arguments to make sure they pass whatever criteria the operator requires.

The type deducer is then invoked on those same arguments to infer what the returned type is from the function call.
