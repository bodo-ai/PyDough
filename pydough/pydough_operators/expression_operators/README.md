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

### [keyword_branching_operators.py](keyword_branching_operators.py)

- `KeywordBranchingExpressionFunctionOperator`: Implementation class for PyDough operators that return an `ExpressionFunctionOperator` and represent a function call that supports keyword arguments, such as `VAR` or `STD`. For example, `VAR` can be set with the keyword argument `type="population"` or `type="sample"`, thereby creating two different operators, `POPULATION_VARIANCE` and `SAMPLE_VARIANCE`.

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

#### Other Operators

These are other PyDough operators that are not necessarily used as functions:

- `SLICE`: operator used for string slicing, with the same semantics as Python string slicing. If `s[a:b:c]` is done, that is translated to `SLICE(s,a,b,c)` in PyDough, and any of `a`/`b`/`c` could be absent. Negative slicing is supported. Currently PyDough does not support providing step values other than 1.

#### Scalar Functions

These functions must be called on singular data as a function.

##### String Functions

- `LOWER`: converts a string to lowercase.
- `UPPER`: converts a string to uppercase.
- `LENGTH`: returns the length of a string.
- `STARTSWITH`: returns whether the first argument string starts with the second argument string.
- `ENDSWITH`: returns whether the first argument string ends with the second argument string.
- `CONTAINS`: returns whether the first argument string contains the second argument string.
- `LIKE`: returns whether the first argument matches the SQL pattern text of the second argument, where `_` is a 1 character wildcard and `%` is an 0+ character wildcard.
- `JOIN_STRINGS`: equivalent to the Python string join method, where the first argument is used as a delimiter to concatenate the remaining arguments.
- `LPAD`: pads the first argument with the second argument to the left until the first argument is equal in length to the third argument.
- `RPAD`: pads the first argument with the second argument to the right until the first argument is equal in length to the third argument.
- `FIND`: returns the index(0-indexed) of the first occurrence of the second argument within the first argument, or -1 if the second argument is not found.
- `STRIP`: returns the first argument with all leading and trailing whitespace removed, including newlines, tabs, and spaces. If the second argument is provided, it is used as the set of characters to remove from the leading and trailing ends of the first argument.
##### Datetime Functions

- `DATETIME`: constructs a new datetime, either from an existing one or the current datetime, and augments it by adding/subtracting intervals of time and/or truncating it to various units.
- `YEAR`: returns the year component of a datetime.
- `MONTH`: returns the month component of a datetime.
- `DAY`: returns the day component of a datetime.
- `HOUR`: Returns the hour component of a datetime.
- `MINUTE`: Returns the minute component of a datetime.
- `SECOND`: Returns the second component of a datetime.
- `DATEDIFF("unit",x,y)`: Returns the difference between two dates (y-x) in one of 
            - **Years**: `"years"`, `"year"`, `"y"`
            - **Months**: `"months"`, `"month"`, `"mm"`
            - **Weeks**: `"weeks"`, `"week"`, `"w"`
            - **Days**: `"days"`, `"day"`, `"d"`
            - **Hours**: `"hours"`, `"hour"`, `"h"`
            - **Minutes**: `"minutes"`, `"minute"`, `"m"`
            - **Seconds**: `"seconds"`, `"second"`, `"s"`.
- `DAYOFWEEK`: returns the day of the week of a datetime in integer form. The config `start_of_week` determines which day is considered the first day of the week. The config `start_week_as_zero` determines whether the week starts at 0 or 1.
- `DAYNAME`: returns the name of the day of the week of a week day represented as an integer. The config `start_of_week` determines which day is considered the first day of the week. The config `start_week_as_zero` determines whether the week starts at 0 or 1.

##### Conditional Functions

- `IFF`: if the first argument is true returns the second argument, otherwise returns the third argument.
- `DEFAULT_TO`: returns the first of its arguments that is non-null.
- `PRESENT`: returns True if the argument is non-null.
- `ABSENT`: returns True if the argument is null.
- `KEEP_IF`: returns the first argument if the second argument is True, otherwise returns null.
- `MONOTONIC`: returns True if each argument is `<=` the next argument.

##### Numeric Functions

- `ABS`: returns the absolute value of the input.
- `ROUND`: rounds the first argument to a number of digits equal to the second argument. If second argument is not provided, the first argument is rounded to 0 decimal places.
- `POWER`: exponentiates the first argument to the power of second argument.
- `SQRT`: returns the square root of the input. 
- `SIGN`: returns the sign of the input. It returns 1 if the input is positive, -1 if the input is negative, and 0 if the input is zero.
- `SMALLEST`: returns the smallest value from the set of values it is called on.
- `LARGEST`: returns the largest value from the set of values it is called on.

#### Aggregation Functions

These functions can be called on plural data to aggregate it into a singular expression.

##### Simple Aggregations

- `SUM`: returns the result of adding all of the non-null values of a plural expression.
- `AVG`: returns the result of taking the average of the non-null values of a plural expression.
- `MEDIAN`: returns the result of taking the median of the non-null values of a plural expression.
- `MIN`: returns the largest out of the non-null values of a plural expression.
- `MAX`: returns the smallest out of the non-null values of a plural expression.
- `ANYTHING`: returns an arbitrary entry from the values of a plural expression.
- `COUNT`: counts how many non-null values exist in a plural expression (special: see collection aggregations).
- `NDISTINCT`: counts how many unique values exist in a plural expression (special: see collection aggregations).
- `VAR`: the basic operation for variance, which is used to create the other variance functions with different types of keyword arguments. Note: `VAR` is not a valid PyDough function operator, but it is used internally to represent the basic variance operation.
- `STD`: the basic operation for standard deviation, which is used to create the other standard deviation functions with different types of keyword arguments. Note: `STD` is not a valid PyDough function operator, but it is used internally to represent the basic standard deviation operation.
- `SAMPLE_VARIANCE`: returns the sample variance of the values of a plural expression.
- `SAMPLE_STD`: returns the sample standard deviation of the values of a plural expression.
- `POPULATION_VARIANCE`: returns the population variance of the values of a plural expression.
- `POPULATION_STD`: returns the population standard deviation of the values of a plural expression.

##### Collection Aggregations

- `COUNT`: if called on a subcollection, returns how many records of it exist for each record of the current collection (if called on an expression instead of collection, see simple aggregations).
- `NDISTINCT`: if called on a subcollection, returns how many distinct records of it exist for each record of the current collection (if called on an expression instead of collection, see simple aggregations).
- `HAS`: called on a subcollection and returns whether any records of the subcollection for each record of the current collection. Equivalent to `COUNT(X) > 0`.
- `HASNOT`: called on a subcollection and returns whether there are no records of the subcollection for each record of the current collection. Equivalent to `COUNT(X) == 0`.

#### Window Functions

These functions return an expression and use logic that produces a value that depends on other records in the collection. Each of these functions has an optional `per` argument. If it is absent, it means that the operation is done by examining all records globally. If `per` is provided, it must be a string describing the name of one of the ancestors of the current context, and if so it indicates that the operation is only done comparing the record against other records that are subcollection entries of the same ancestor collection. If there are multiple ancestors with that name, a suffix `:idx` is included to specify which one is used, with smaller numbers indicating more recent ancestors (e.g. `sizes:1` means look for the most recent ancestor with the name `sizes`, and `nations:2` means look for the 2nd most recent ancestor witht he name `nations`).

- `RANKING(by=..., per=None, allow_ties=False, dense=False)`: returns the ordinal position of the current record when all records are sorted by the collation expressions in the `by` argument. By default, uses the same semantics as `ROW_NUMBER`. If `allow_ties=True`, instead uses `RANK`. If `allow_ties=True` and `dense=True`, instead uses `DENSE_RANK`.
- `PERCENTILE(by=..., per=None, n_buckets=100)`: splits the data into `n_buckets` equal sized sections by ordering the data by the `by` arguments, where bucket `1` is the smallest data and bucket `n_buckets` is the largest. This is useful for understanding the relative position of a value within a group, like finding the top 10% of performers in a class.
- `PREV(expr, n=1, default=None, by=..., per=None)`: returns the nth-preceding value of `expr` within the group of data specified by the `per` argument, when sorted by the `by` argument. If there are not `n` preceding values, returns `default` instead.
- `NEXT(expr, n=1, default=None, by=..., per=None)`: same as `PREV` but in the opposite direction.
- `RELSUM(expr, per=None)`: returns the sum of the values of `expr` within the group of data specified by `per`.
- `RELAVG(expr, per=None)`: returns the average of the values of `expr` within the group of data specified by `per`.
- `RELCOUNT(expr, per=None)`: returns the number of non-null values of `expr` within the group of data specified by `per`.
- `RELSIZE(per=None)`: returns the number of records of `expr` (null or non-null) within the group of data specified by `per`.

For an example of how `per` works, when doing `Regions.nations.customers.CALCULATE(r=RANKING(by=...))`:

- If `per=None`, `r` is the ranking across all `customers`.
- If `per="nations"`, `r` is the ranking of customers per-nation (meaning the ranking resets to 1 within each nation).
- If `per="Regions"`, `r` is the ranking of customers per-region (meaning the ranking resets to 1 within each region).

#### Casting Functions

- `STRING`: casts the first argument to the second argument. If a second argument is provided, it is used as the datetime format string that gets passed to the underlying database.
- `INTEGER`: casts the argument to an integer.
- `FLOAT`: casts the argument to a float.

## Interaction with Type Inference

Expression operators interact with the type inference module to ensure that the arguments passed to them are valid and to infer the return types of those expressions. This helps maintain type safety and correctness in PyDough operations. Every operator has a type verifier object and a type deducer object.

The type verifier is invoked whenever the operator is used in a function call expression with QDAG arguments to make sure they pass whatever criteria the operator requires.

The type deducer is then invoked on those same arguments to infer what the returned type is from the function call.
