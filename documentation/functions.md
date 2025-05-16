# PyDough Functions List

Below is the list of every function/operator currently supported in PyDough as a builtin.

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Binary Operators](#binary-operators)
   * [Arithmetic](#arithmetic)
   * [Comparisons](#comparisons)
   * [Logical](#logical)
- [Unary Operators](#unary-operators)
   * [Negation](#negation)
- [Other Operators](#other-operators)
   * [Slicing](#slicing)
- [String Functions](#string-functions)
   * [LOWER](#lower)
   * [UPPER](#upper)
   * [LENGTH](#length)
   * [STARTSWITH](#startswith)
   * [ENDSWITH](#endswith)
   * [CONTAINS](#contains)
   * [LIKE](#like)
   * [JOIN_STRINGS](#join_strings)
   * [LPAD](#lpad)
   * [RPAD](#rpad)
   * [FIND](#find)
   * [STRIP](#strip)
- [Datetime Functions](#datetime-functions)
   * [DATETIME](#datetime)
   * [YEAR](#year)
   * [QUARTER](#quarter)
   * [MONTH](#month)
   * [DAY](#day)
   * [HOUR](#hour)
   * [MINUTE](#minute)
   * [SECOND](#second)
   * [DATEDIFF](#datediff)
   * [DAYOFWEEK](#dayofweek)
   * [DAYNAME](#dayname)
- [Conditional Functions](#conditional-functions)
   * [IFF](#iff)
   * [ISIN](#isin)
   * [DEFAULT_TO](#default_to)
   * [PRESENT](#present)
   * [ABSENT](#absent)
   * [KEEP_IF](#keep_if)
   * [MONOTONIC](#monotonic)
- [Numerical Functions](#numerical-functions)
   * [ABS](#abs)
   * [ROUND](#round)
   * [POWER](#power)
   * [SQRT](#sqrt)
   * [SIGN](#sign)
   * [SMALLEST](#smallest)
   * [LARGEST](#largest)
- [Aggregation Functions](#aggregation-functions)
   * [SUM](#sum)
   * [AVG](#avg)
   * [MEDIAN](#median)
   * [MIN](#min)
   * [MAX](#max)
   * [ANYTHING](#anything)
   * [COUNT](#count)
   * [NDISTINCT](#ndistinct)
   * [HAS](#has)
   * [HASNOT](#hasnot)
   * [VAR](#var)
   * [STD](#std)
- [Window Functions](#window-functions)
   * [RANKING](#ranking)
   * [PERCENTILE](#percentile)
   * [PREV](#prev)
   * [NEXT](#next)
   * [RELSUM](#relsum)
   * [RELAVG](#relavg)
   * [RELCOUNT](#relcount)
   * [RELSIZE](#relsize)
- [Casting Functions](#casting-functions)
   * [STRING](#string)
   * [INTEGER](#integer)
   * [FLOAT](#float)
- [Banned Python Logic](#banned-python-logic)
   * [\_\_bool\_\_](#__bool__)
   * [\_\_call\_\_](#call_banned)
   * [\_\_floor\_\_](#floor_banned)
   * [\_\_ceil\_\_](#ceil_banned)
   * [\_\_trunc\_\_](#trunc_banned)
   * [\_\_reversed\_\_](#reversed_banned)
   * [\_\_int\_\_](#int_banned)
   * [\_\_float\_\_](#float_banned)
   * [\_\_complex\_\_](#complex_banned)
   * [\_\_index\_\_](#index_banned)
   * [\_\_len\_\_](#len_banned)
   * [\_\_contains\_\_](#contains_banned)
   * [\_\_setitem\_\_](#setitem_banned)

<!-- TOC end -->

<!-- TOC --><a name="binary-operators"></a>

## Binary Operators

Below is each binary operator currently supported in PyDough.

<!-- TOC --><a name="arithmetic"></a>

### Arithmetic

Supported mathematical operations: addition (`+`), subtraction (`-`), multiplication (`*`), division (`/`), exponentiation (`**`).

```py
Lineitems.CALCULATE(value = (extended_price * (1 - (discount ** 2)) + 1.0) / part.retail_price)
```

> [!WARNING]
> The behavior when the denominator is `0` depends on the database being used to evaluate the expression.

<!-- TOC --><a name="comparisons"></a>

### Comparisons

Expression values can be compared using standard comparison operators: `<=`, `<`, `==`, `!=`, `>` and `>=`:

```py
customers.CALCULATE(
    in_debt = acctbal < 0,
    at_most_12_orders = COUNT(orders) <= 12,
    is_european = nation.region.name == "EUROPE",
    non_german = nation.name != "GERMANY",
    non_empty_acct = acctbal > 0,
    at_least_5_orders = COUNT(orders) >= 5,
)
```

> [!WARNING]
> Chained inequalities, like `a <= b <= c`, can cause undefined/incorrect behavior in PyDough. Instead, use expressions like `(a <= b) & (b <= c)`, or the [MONOTONIC](#monotonic) function.

<!-- TOC --><a name="logical"></a>

### Logical

Multiple boolean expression values can be logically combined with `&`, `|` and `~` being used as logical AND, OR and NOT, respectively:

```py
is_asian = nation.region.name == "ASIA"
is_european = nation.region.name == "EUROPE"
in_debt = acctbal < 0
customers.CALCULATE(
    is_eurasian = is_asian | is_european,
    is_not_eurasian = ~(is_asian | is_european),
    is_european_in_debt = is_european & in_debt
)
```

> [!WARNING]
> Do **NOT** use the builtin Python syntax `and`, `or`, or `not` on PyDough node. Using these instead of `&`, `|` or `~` can result in undefined incorrect results.

<!-- TOC --><a name="unary-operators"></a>

## Unary Operators

Below is each unary operator currently supported in PyDough.

<!-- TOC --><a name="negation"></a>

### Negation

A numerical expression's sign can be flipped by prefixing it with the `-` operator:

```py
Lineitems.CALCULATE(lost_value = extended_price * (-discount))
```

<!-- TOC --><a name="other-operators"></a>

## Other Operators

Below are all other operators currently supported in PyDough that use other syntax besides function calls:

<!-- TOC --><a name="slicing"></a>

### Slicing

A string expression can have a substring extracted with Python string slicing syntax `s[a:b:c]`.
The implementation is based on Python slicing semantics. PyDough supports negative slicing, but currently, it does not support providing step values other than 1.

```py
customers.CALCULATE(
    country_code = phone[:3],
    name_without_first_char = name[1:],
    last_digit = phone[-1:],
    name_without_start_and_end_char = name[1:-1]
    phone_without_last_5_chars = phone[:-5]
    name_second_to_last_char = name[-2:-1]
)
```

> [!WARNING]
> PyDough currently only supports combinations of `string[start:stop:step]` where `step` is either 1 or omitted, and both `start` and `stop` are either non-negative values or omitted.

<!-- TOC --><a name="string-functions"></a>

## String Functions

Below is each function currently supported in PyDough that operates on strings.

<!-- TOC --><a name="lower"></a>

### LOWER

Calling `LOWER` on a string converts its characters to lowercase:

```py
customers.CALCULATE(lowercase_name = LOWER(name))
```

<!-- TOC --><a name="upper"></a>

### UPPER

Calling `UPPER` on a string converts its characters to uppercase:

```py
customers.CALCULATE(uppercase_name = UPPER(name))
```

<!-- TOC --><a name="length"></a>

### LENGTH

Calling `length` on a string returns the number of characters it contains:

```py
Suppliers.CALCULATE(n_chars_in_comment = LENGTH(comment))
```

<!-- TOC --><a name="startswith"></a>

### STARTSWITH

The `STARTSWITH` function checks if its first argument begins with its second argument as a string prefix:

```py
Parts.CALCULATE(begins_with_yellow = STARTSWITH(name, "yellow"))
```

<!-- TOC --><a name="endswith"></a>

### ENDSWITH

The `ENDSWITH` function checks if its first argument ends with its second argument as a string suffix:

```py
Parts.CALCULATE(ends_with_chocolate = ENDSWITH(name, "chocolate"))
```

<!-- TOC --><a name="contains"></a>

### CONTAINS

The `CONTAINS` function checks if its first argument contains its second argument as a substring:

```py
Parts.CALCULATE(is_green = CONTAINS(name, "green"))
```

<!-- TOC --><a name="like"></a>

### LIKE

The `LIKE` function checks if the first argument matches the SQL pattern text of the second argument, where `_` is a 1 character wildcard and `%` is an 0+ character wildcard.

```py
Orders.CALCULATE(is_special_request = LIKE(comment, "%special%requests%"))
```

[This link](https://www.w3schools.com/sql/sql_like.asp) explains how these SQL pattern strings work and provides some examples.

<!-- TOC --><a name="join_strings"></a>

### JOIN_STRINGS

The `JOIN_STRINGS` function concatenates all its string arguments, using the first argument as a delimiter between each of the following arguments (like the `.join` method in Python):

```py
Regions.CALCULATE(
   region_name=name
).nations.CALCULATE(
   nation_name=name
).customers.CALCULATE(
   fully_qualified_name = JOIN_STRINGS("-", region_name, nation_name, name)
)
```

For instance, `JOIN_STRINGS("; ", "Alpha", "Beta", "Gamma)` returns `"Alpha; Beta; Gamma"`.

<!-- TOC --><a name="lpad"></a>
### LPAD

The `LPAD` function pads an expression on the left side with a specified padding character until it is the desired length. It takes three arguments:

1. The input string to pad.
2. The desired final length of the expression. It should be a positive integer literal.
3. The single character literal to use for padding.

The function behaves as follows:

- If the input expression is shorter than the desired length, it adds padding characters on the left until reaching the desired length. 
- If the input expression is longer than the desired length, it truncates the expression by removing characters from the right side until it matches the desired length.
- If the desired length is 0, it returns an empty string.
- If the desired length is negative, it raises an error.
- If the padding argument is not a single character, it raises an error.

```py
customers.CALCULATE(left_padded_name = LPAD(name, 30, "*"))
```

Here are examples on how it pads on string literals:
| Input | Output |
|-------|--------|
| `LPAD("123", 6, "0")` | `"000123"` |
| `LPAD("123", 5, "#")` | `"##123"` |
| `LPAD("123", 3, "0")` | `"123"` |
| `LPAD("123", 2, "0")` | `"12"` |
| `LPAD("123", 0, "0")` | `""` |

<!-- TOC --><a name="rpad"></a>
### RPAD

The `RPAD` function pads an expression on the right side with a specified padding character until it is the desired length. It takes three arguments:

1. The input string to pad.
2. The desired final length of the expression. It should be a positive integer literal.
3. The single character literal to use for padding.

The function behaves as follows:

- If the input expression is shorter than the desired length, it adds padding characters on the right until reaching the desired length.
- If the input expression is longer than the desired length, it truncates the expression by removing characters from the right side until it matches the desired length.
- If the desired length is 0, it returns an empty string
- If the desired length is negative, it raises an error
- If the padding argument is not a single character, it raises an error

```py
customers.CALCULATE(right_padded_name = RPAD(name, 30, "*"))
```

Here are examples on how it pads on string literals:
| Input | Output |
|-------|--------|
| `RPAD("123", 6, "0")` | `"123000"` |
| `RPAD("123", 5, "#")` | `"123##"` |
| `RPAD("123", 3, "0")` | `"123"` |
| `RPAD("123", 2, "0")` | `"12"` |
| `RPAD("123", 0, "0")` | `""` |

<!-- TOC --><a name="find"></a>

### FIND

The `FIND` function returns the position (0-indexed) of the first occurrence of a substring within a string, or -1 if the substring is not found. The first argument is the string to search within, and the second argument is the substring to search for.

```py
customers.WHERE(name == "Alex Rodriguez")
         .CALCULATE(
            idx_Alex = FIND(name, "Alex"), # 0
            idx_Rodriguez = FIND(name, "Rodriguez"), # 5
            idx_bob = FIND(name, "bob"), # -1
            idx_e = FIND(name, "e"), # 2
            idx_space = FIND(name, " "), # 4
            idx_of_R = FIND(name, "R"), # 5
            idx_of_Alex_Rodriguez = FIND(name, "Alex Rodriguez"), # 0
)
```

<!-- TOC --><a name="strip"></a>

### STRIP

The `STRIP` function returns the first argument with all leading and trailing whitespace removed, including newlines, tabs, and spaces.
If the second argument is provided, it is used as the set of characters to remove from the leading and trailing ends of the first argument.
It continues removing characters until it encounters a character that is not in the set.
This function is equivalent to python's `str.strip()` method.
Note: This function is case-sensitive.

```py
customers.CALCULATE(stripped_name = STRIP(name)) # removes all leading and trailing whitespace
customers.CALCULATE(stripped_name = STRIP(name, "aeiou")) # removes all leading and trailing vowels
```

| **Input String (X)**       | **STRIP(X, Y)**                    | **Result**          |
|-----------------------------|---------------------------------------|---------------------|
| `'abcXYZcba'`              | `STRIP('abcXYZcba','abc')`        | `'XYZ'`            |
| `'$$Hello$$'`              | `STRIP('$$Hello$$','$$')`          | `'Hello'`          |
| `'---Test-String---'`      | `STRIP('---Test-String---','-')`  | `'Test-String'`    |
| `'123456Hello654321'`      | `STRIP('123456Hello654321','123456')` | `'Hello'`         |

<!-- TOC --><a name="datetime-functions"></a>

## Datetime Functions

Below is each function currently supported in PyDough that operates on date/time/timestamp values.

<!-- TOC --><a name="datetime"></a>
### DATETIME

The `DATETIME` function is used to build/augment date/timestamp values. The first argument is the base date/timestamp, and it can optionally take in a variable number of modifier arguments.

The base argument can be one of the following:

- A string literal indicating that the current timestamp should be built, which has to be one of the following: `now`, `current_date`, `current_timestamp`, `current date`, `current timestamp`, `currentdate` or `currenttimestamp`. All of these aliases are equivalent, case-insensitive, and ignore leading/trailing whitespace.
- A string literal representing datetime data (e.g. `"2024-01-01"` or `199-12-31 12:59:30`).
- A datetime literal (e.g. `datetime.date`, `datetime.datetime`, or a `pd.Timestamp` from pandas).
- A column of datetime data.

> [!NOTE]
> Other datetime functions ([DATEDIFF](#datediff), [YEAR](#year), [QUARTER](#quarter), [MONTH](#month), [DAY](#day), [HOUR](#hour), [MINUTE](#minute) or [SECOND](#second)) also allow any of the base arguments above as datetime values. For example, you can call `YEAR("now")`, `DATEDIFF("months", dt, pd.Timestamp("2024-03-14")))`, `MONTH("1999-06-13")`, or `DATEDIFF("days", datetime.date(2025, 1, 1), "now")`.

The modifier arguments can be the following (all of the options are case-insensitive and ignore leading/trailing/extra whitespace):
- A string literal in the format `start of <UNIT>` indicating to truncate the datetime value to a certain unit, which can be the following:
  - **Years**: Supported aliases are `"years"`, `"year"`, and `"y"`.
  - **Quarters**: Supported aliases are `"quarters"`, `"quarter"`, and `"q"`.
  - **Months**: Supported aliases are `"months"`, `"month"`, and `"mm"`.
  - **Days**: Supported aliases are `"days"`, `"day"`, and `"d"`.
  - **Weeks**: Supported aliases are `"weeks"`, `"week"`, and `"w"`.
  - **Hours**: Supported aliases are `"hours"`, `"hour"`, and `"h"`.
  - **Minutes**: Supported aliases are `"minutes"`, `"minute"`, and `"m"`.
  - **Seconds**: Supported aliases are `"seconds"`, `"second"`, and `"s"`.
- A string literal in the form `±<AMT> <UNIT>` indicating to add/subtract a date/time interval to the datetime value. The sign can be `+` or `-`, and if omitted the default is `+`. The amount must be an integer. The unit must be one of the same unit strings allowed for truncation.

For example, `"Days"`, `"DAYS"`, and `"d"` are all treated the same due to case insensitivity.

If there are multiple modifiers, they operate left-to-right.

```py
# Returns the following datetime moments:
# 1. The current timestamp
# 2. The start of the current month
# 3. Exactly 12 hours from now
# 4. The last day of the previous year
# 5. The current day, at midnight
# 6. The first day after the start of the current quarter
TPCH.CALCULATE(
   ts_1=DATETIME('now'),
   ts_2=DATETIME('NoW', 'start of month'),
   ts_3=DATETIME(' CURRENT_DATE ', '12 hours'),
   ts_4=DATETIME('Current Timestamp', 'start of y', '- 1 D'),
   ts_5=DATETIME('NOW', '  Start  of  Day  '),
   ts_6=DATETIME('now', 'start of quarter', '+1 d'),
)

# For each order, truncates the order date to the first day of the year
Orders.CALCULATE(order_year=DATETIME(order_year, 'START OF Y'))
```

<!-- TOC --><a name="year"></a>

### YEAR

Calling `YEAR` on a date/timestamp extracts the year it belongs to:

```py
Orders.WHERE(YEAR(order_date) == 1995)
```

<!-- TOC --><a name="quarter"></a>

### QUARTER

Calling `QUARTER` on a date/timestamp extracts the quarter of the year it belongs to. The range of output is from 1-4. Months 1-3 are part of quarter 1, months 4-6 are part of quarter 2, months 7-9 are part of quarter 3, and months 10-12 are part of quarter 4.

```py
Orders.WHERE(QUARTER(order_date) == 1)
```

<!-- TOC --><a name="month"></a>

### MONTH

Calling `MONTH` on a date/timestamp extracts the month of the year it belongs to:

```py
Orders.CALCULATE(is_summer = (MONTH(order_date) >= 6) & (MONTH(order_date) <= 8))
```

<!-- TOC --><a name="day"></a>

### DAY

Calling `DAY` on a date/timestamp extracts the day of the month it belongs to:

```py
Orders.CALCULATE(is_first_of_month = DAY(order_date) == 1)
```

<!-- TOC --><a name="hour"></a>

### HOUR

Calling `HOUR` on a date/timestamp extracts the hour it belongs to. The range of output
is from 0-23:

```py
Orders.CALCULATE(is_12pm = HOUR(order_date) == 12)
```

<!-- TOC --><a name="minute"></a>

### MINUTE

Calling `MINUTE` on a date/timestamp extracts the minute. The range of output
is from 0-59:

```py
Orders.CALCULATE(is_half_hour = MINUTE(order_date) == 30)
```

<!-- TOC --><a name="second"></a>

### SECOND

Calling `SECOND` on a date/timestamp extracts the second. The range of output
is from 0-59:

```py
Orders.CALCULATE(is_lt_30_seconds = SECOND(order_date) < 30)
```

<!-- TOC --><a name="datediff"></a>

### DATEDIFF

Calling `DATEDIFF` between 2 timestamps returns the difference in one of `years`, `quarters`, `months`, `weeks`, `days`, `hours`, `minutes` or `seconds`.

- `DATEDIFF("years", x, y)`: Returns the **number of full years since x that y occurred**. For example, if **x** is December 31, 2009, and **y** is January 1, 2010, it counts as **1 year apart**, even though they are only 1 day apart.
- `DATEDIFF("quarters", x, y)`: Returns the **number of full quarters since x that y occurred**. For example, if **x** is March 31, 2014, and **y** is April 1, 2014, it counts as **1 quarter apart**, even though they are only 1 day apart.
- `DATEDIFF("months", x, y)`: Returns the **number of full months since x that y occurred**. For example, if **x** is January 31, 2014, and **y** is February 1, 2014, it counts as **1 month apart**, even though they are only 1 day apart.
- `DATEDIFF("weeks", x, y)`: Returns the **number of full weeks since x that y occurred**. The dates x and y are first truncated to the start of week (as specified by the `start_of_week` config), then the difference in number of full weeks is calculated (a week is defined as 7 days). For example, if `start_of_week` is set to Saturday:
  ```python
  # If x is "2025-03-18" (Tuesday) and y is "2025-03-31" (Monday)
  DATEDIFF("weeks", x, y) returns 2
  ```
 Please see the [Session Configs](./usage.md#session-configs) documentation for more details and in-depth examples.
- `DATEDIFF("days", x, y)`: Returns the **number of full days since x that y occurred**. For example, if **x** is 11:59 PM on one day, and **y** is 12:01 AM the next day, it counts as **1 day apart**, even though they are only 2 minutes apart.
- `DATEDIFF("hours", x, y)`: Returns the **number of full hours since x that y occurred**. For example, if **x** is 6:59 PM and **y** is 7:01 PM on the same day, it counts as **1 hour apart**, even though the difference is only 2 minutes.
- `DATEDIFF("minutes", x, y)`: Returns the **number of full minutes since x that y occurred**. For example, if **x** is 7:00 PM and **y** is 7:01 PM, it counts as **1 minute apart**, even though the difference is exactly 60 seconds.
- `DATEDIFF("seconds", x, y)`: Returns the **number of full seconds since x that y occurred**. For example, if **x** is at 7:00:01 PM and **y** is at 7:00:02 PM, it counts as **1 second apart**.

```py
# Calculates, for each order, the number of days since January 1st 1992
# that the order was placed:
Orders.CALCULATE( 
   days_since=DATEDIFF("days",datetime.date(1992, 1, 1), order_date)
)
```

The first argument in the `DATEDIFF` function supports the following aliases for each unit of time. The argument is **case-insensitive**, and if a unit is not one of the provided options, an error will be thrown. See [`DATETIME`](#datetime) for the supported units and their aliases. Invalid or unrecognized units will result in an error.

<!-- TOC --><a name="dayofweek"></a>

### DAYOFWEEK

The `DAYOFWEEK` function returns the day of the week for a given date/timestamp. It takes a single argument, which is a date/timestamp, and returns an integer between 1 and 7, or between 0 and 6, depending on the `start_of_week` config and `start_week_as_zero` config. Please see the [Session Configs](./usage.md#session-configs) documentation for more details and in-depth examples.

In other words, `DAYOFWEEK` returns which day of the week is the given date/timestamp, where the first day of the give date/timestamp is decided by the `start_of_week` config.

```py
# Returns the day of the week for the order date
Orders.CALCULATE(day_of_week = DAYOFWEEK(order_date))
```

The following table shows the day of the week for a given date/timestamp, where the first day of the give date/timestamp is decided by the `start_of_week` config and if the week starts at 0 or 1 decided by the `start_week_as_zero` config.

| Day of Week  | Start is Sunday, Start at 0 | Start is Sunday, Start at 1 | Start is Monday, Start at 0 | Start is Monday, Start at 1 |
|--------------|-----------------------------|-----------------------------|-----------------------------|-----------------------------|
| Sunday       | 0                           | 1                           | 6                           | 7                           |
| Monday       | 1                           | 2                           | 0                           | 1                           |
| Tuesday      | 2                           | 3                           | 1                           | 2                           |
| Wednesday    | 3                           | 4                           | 2                           | 3                           |
| Thursday     | 4                           | 5                           | 3                           | 4                           |
| Friday       | 5                           | 6                           | 4                           | 5                           |
| Saturday     | 6                           | 7                           | 5                           | 6                           |


<!-- TOC --><a name="dayname"></a>

### DAYNAME

The `DAYNAME` function returns the name of the day of the week for a given date/timestamp. It takes a single argument, which is a date/timestamp, and returns a string, corresponding to the name of the day of the week. This returns one of the following: `"Monday"`, `"Tuesday"`, `"Wednesday"`, `"Thursday"`, `"Friday"`, `"Saturday"`, or `"Sunday"`.

```py
# Returns the name of the day of the week for the order date
Orders.CALCULATE(day_name = DAYNAME(order_date))
```

<!-- TOC --><a name="conditional-functions"></a>

## Conditional Functions

Below is each function currently supported in PyDough that handles conditional logic.

<!-- TOC --><a name="iff"></a>

### IFF

The `IFF` function cases on the True/False value of its first argument. If it is True, it returns the second argument, otherwise it returns the third argument. In this way, the PyDough code `IFF(a, b, c)` is semantically the same as the SQL expression `CASE WHEN a THEN b ELSE c END`.

```py
qty_from_germany = IFF(supplier.nation.name == "GERMANY", quantity, 0)
customers.CALCULATE(
    total_quantity_shipped_from_germany = SUM(lines.CALCULATE(q=qty_from_germany).q)
)
```

<!-- TOC --><a name="isin"></a>

### ISIN

The `ISIN` function takes in an expression and an iterable of literals and returns whether the expression is a member of provided literals.

```py
Parts.WHERE(ISIN(size, (10, 11, 17, 19, 45)))
```

<!-- TOC --><a name="default_to"></a>

### DEFAULT_TO

The `DEFAULT_TO` function returns the first of its arguments that is non-null (e.g. the same as the `COALESCE` function in SQL):

```py
Lineitems.CALCULATE(adj_tax = DEFAULT_TO(tax, 0))
```

<!-- TOC --><a name="present"></a>

### PRESENT

The `PRESENT` function checks if its argument is non-null (e.g. the same as `IS NOT NULL` in SQL):

```py
Lineitems.CALCULATE(has_tax = PRESENT(tax))
```

<!-- TOC --><a name="absent"></a>

### ABSENT

The `ABSENT` function checks if its argument is null (e.g. the same as `IS NULL` in SQL):

```py
Lineitems.CALCULATE(no_tax = ABSENT(tax))
```

<!-- TOC --><a name="keep_if"></a>

### KEEP_IF

The `KEEP_IF` function returns the first function if the second arguments is True, otherwise it returns a null value. In other words, `KEEP_IF(a, b)` is equivalent to the SQL expression `CASE WHEN b THEN a END`.

```py
TPCH.CALCULATE(avg_non_debt_balance = AVG(customers.CALCULATE(no_debt_bal = KEEP_IF(acctbal, acctbal > 0)).no_debt_bal))
```

<!-- TOC --><a name="monotonic"></a>

### MONOTONIC

The `MONOTONIC` function checks if all of its arguments are in ascending order (e.g. `MONOTONIC(a, b, c, d)` is equivalent to `(a <= b) & (b <= c) & (c <= d)`):

```py
Lineitems.WHERE(MONOTONIC(10, quantity, 20) & MONOTONIC(5, part.size, 13))
```

<!-- TOC --><a name="numerical-functions"></a>

## Numerical Functions

Below is each numerical function currently supported in PyDough.

<!-- TOC --><a name="abs"></a>

### ABS

The `ABS` function returns the absolute value of its input. The Python builtin `abs()` function can also be used to accomplish the same thing.

```py
customers.CALCULATE(acct_magnitude = ABS(acctbal))
# The below statement is equivalent to above.
customers.CALCULATE(acct_magnitude = abs(acctbal))
```

<!-- TOC --><a name="round"></a>

### ROUND

The `ROUND` function rounds its first argument to the precision of its second argument. The rounding rules used depend on the database's round function. The second argument is optional, and if not provided, the first argument is rounded to 0 decimal places. The Python builtin `round()` function can also be used to accomplish the same thing. 

```py
Parts.CALCULATE(rounded_price = ROUND(retail_price, 1))
# The below statement is equivalent to above.
Parts.CALCULATE(rounded_price = round(retail_price, 1))

# The below statement takes the default precision as 0.
Parts.CALCULATE(rounded_price = ROUND(retail_price))
# The below statement is equivalent to above.
Parts.CALCULATE(rounded_price = ROUND(retail_price,0))
```

Note: The default precision for builtin `round` method is 0, to be in alignment with the Python implementation. The PyDough `ROUND` function requires the precision to be specified.

```py
# This is legal.
Parts.CALCULATE(rounded_price = round(retail_price))
# This is illegal as precision is not specified.
Parts.CALCULATE(rounded_price = ROUND(retail_price))
```

<!-- TOC --><a name="power"></a>

### POWER

The `POWER` function exponentiates its first argument to the power of its second argument.

```py
Parts.CALCULATE(powered_price = POWER(retail_price, 2))
```

<!-- TOC --><a name="sqrt"></a>

### SQRT

The `SQRT` function takes the square root of its input. It's equivalent to `POWER(x,0.5)`.

```py
Parts.CALCULATE(sqrt_price = SQRT(retail_price))
```

<!-- TOC --><a name="sign"></a>

### SIGN

The `SIGN` function returns the sign of its input. It returns 1 if the input is positive, -1 if the input is negative, and 0 if the input is zero.

```py
Suppliers.CALCULATE(sign_of_acctbal = SIGN(account_balance))
```

<!-- TOC --><a name="smallest"></a>

### SMALLEST

The `SMALLEST` function returns the smallest value from the set of values it is called on. It can take in a variable number of arguments, but at least two arguments are required. If any of the arguments are `NULL`, the function will return `NULL`.

```py
TPCH.CALCULATE(
    s1=SMALLEST(20,10,10,-1,-2,100,-200), # Returns -200
    s2=SMALLEST(-0.001,-0.01,-0.0001), # Returns -0.0001
    s3=SMALLEST(datetime.datetime(2025,1,1),datetime.datetime(2024,1,1)), # Returns 2024-01-01
    s4=SMALLEST(1,None,3,4), # Returns NULL
)
# Average gap, in days, for shipments between when they were ordered
# versus when they were expected to arrive (or when they actually arrived,
# if they were early), for shipments done via rail.
delay_info = Lineitems.WHERE(HAS(order) & (ship_mode == "RAIL")).CALCULATE(
   day_gap=DATEDIFF("days", order.order_date, SMALLEST(commit_date, receipt_date))
)
return TPCH.CALCULATE(avg_gap=AVG(delay_info.day_gap))
```

<!-- TOC --><a name="largest"></a>

### LARGEST

The `LARGEST` function returns the largest value from the set of values it is called on. It can take in a variable number of arguments, but at least two arguments are required. If any of the arguments are `NULL`, the function will return `NULL`.

```py
TPCH.CALCULATE(
    l1=LARGEST(20,10,10,-1,-2,100,-200), # Returns 100
    l2=LARGEST(-0.001,-0.01,-0.0001), # Returns -0.01
    l3=LARGEST(datetime.datetime(2025,1,1),datetime.datetime(2024,1,1)), # Returns 2025-01-01
    l4=LARGEST(1,None,3,4), # Returns NULL
)
# For each region, what is the average account balance of all
# customers in a hypothetical scenario where all debt was erased
Regions.CALCULATE(
   region_name=name,
   avg_bal_without_debt_erasure=AVG(LARGEST(nations.customers.acctbal, 0)),
)
```

<!-- TOC --><a name="aggregation-functions"></a>

## Aggregation Functions

When terms of a plural sub-collection are accessed, those terms are plural with regards to the current collection. For example, if each nation in `Nations` has multiple `customers`, and each customer has a single `acctbal`, then `customers.acctbal` is plural with regards to `Nations` and cannot be used in any calculations when the current context is `Nations`. The exception to this is when `customers.acctbal` is made singular with regards to `Nations` by aggregating it.

Aggregation functions are a special set of functions that, when called on their inputs, convert them from plural to singular. Below is each aggregation function currently supported in PyDough.

<!-- TOC --><a name="sum"></a>

### SUM

The `SUM` function returns the sum of the plural set of numerical values it is called on.

```py
Nations.CALCULATE(total_consumer_wealth = SUM(customers.acctbal))
```

<!-- TOC --><a name="avg"></a>

### AVG

The `AVG` function takes the average of the plural set of numerical values it is called on.

```py
Parts.CALCULATE(average_shipment_size = AVG(lines.quantity))
```

<!-- TOC --><a name="median"></a>

### MEDIAN

The `MEDIAN` function takes the median of the plural set of numerical values it is called on.
Note: absent records are ignored when deriving the median.

```py
customers.CALCULATE(
   name,
   median_order_price = MEDIAN(orders.total_price)
)
```

<!-- TOC --><a name="min"></a>

### MIN

The `MIN` function returns the smallest value from the set of values it is called on.

```py
Suppliers.CALCULATE(cheapest_part_supplied = MIN(supply_records.supply_cost))
```

<!-- TOC --><a name="max"></a>

### MAX

The `MAX` function returns the largest value from the set of values it is called on.

```py
Suppliers.CALCULATE(most_expensive_part_supplied = MAX(supply_records.supply_cost))
```

<!-- TOC --><a name="anything"></a>

### ANYTHING

The `ANYTHING` function returns an arbitrary value from the set of values it is called on.

```py
Suppliers.CALCULATE(chosen_part_name = ANYTHING(supply_records.part.name))
```

<!-- TOC --><a name="count"></a>

### COUNT

The `COUNT` function returns how many non-null records exist on the set of plural values it is called on.

```py
customers.CALCULATE(num_taxed_purchases = COUNT(orders.lines.tax))
```

The `COUNT` function can also be called on a sub-collection, in which case it will return how many records from that sub-collection exist.

```py
Nations.CALCULATE(num_customers_in_debt = COUNT(customers.WHERE(acctbal < 0)))
```

<!-- TOC --><a name="ndistinct"></a>

### NDISTINCT

The `NDISTINCT` function returns how many distinct values of its argument exist.

```py
customers.CALCULATE(num_unique_parts_purchased = NDISTINCT(orders.lines.parts.key))
```

<!-- TOC --><a name="has"></a>

### HAS

The `HAS` function is called on a sub-collection and returns `True` if at least one record of the sub-collection exists. In other words, `HAS(x)` is equivalent to `COUNT(x) > 0`.

```py
Parts.WHERE(HAS(supply_records.supplier.WHERE(nation.name == "GERMANY")))
```

<!-- TOC --><a name="hasnot"></a>

### HASNOT

The `HASNOT` function is called on a sub-collection and returns `True` if no records of the sub-collection exist. In other words, `HASNOT(x)` is equivalent to `COUNT(x) == 0`.

```py
customers.WHERE(HASNOT(orders))
```

<!-- TOC --><a name="var"></a>

### VAR

The `VAR` function returns the variance of the set of numerical values it is called on. This operation supports the `type` keyword argument, which can be used to specify the type of variance to compute. The following variances are supported:

- `"population"` (default value): The population variance.
- `"sample"`: The sample variance.

```py
# Compute the population variance
Parts.CALCULATE(variance = VAR(supply_records.supply_cost))

# Compute the sample variance
Parts.CALCULATE(variance = VAR(supply_records.supply_cost, type="sample"))
```

<!-- TOC --><a name="std"></a>

### STD

The `STD` function returns the standard deviation of the set of numerical values it is called on. This operation supports the `type` keyword argument, which can be used to specify the type of standard deviation to compute. The following standard deviations are supported:

- `"population"` (default value): The population standard deviation.
- `"sample"`: The sample standard deviation.

```py
# Compute the population standard deviation
Parts.CALCULATE(std = STD(supply_records.supply_cost))

# Compute the sample standard deviation
Parts.CALCULATE(std = STD(supply_records.supply_cost, type="sample"))
```

<!-- TOC --><a name="window-functions"></a>

## Window Functions

Window functions are special functions whose output depends on other records in the same context.A common example of this is finding the ranking of each record if all of the records were to be sorted.

Window functions in PyDough have an optional `per` argument. If this argument is omitted, it means that the window function applies to all records of the current collection (e.g. rank all customers). If it is provided, it should be a string that describes which ancestor of the current context the window function should be calculated with regards to, and in that case it means that the set of values used by the window function should be per-record of the correspond ancestor (e.g. rank all customers per-nation).

For example, if using the `RANKING` window function, consider the following examples:

```py
# Rank every customer relative to all other customers byacctbal
Regions.nations.customers.CALCULATE(r=RANKING(by=acctbal.DESC()))

# Rank every customer relative to other customers in the same nation, by acctbal
Regions.nations.customers.CALCULATE(r=RANKING(by=acctbal.DESC(), per="nations"))

# Rank every customer relative to other customers in the same region, by acctbal
Regions.nations.customers.CALCULATE(r=RANKING(by=acctbal.DESC(), per="Regions"))
```

If there are multiple ancestors of the current context with the same name, the `per` string should include a suffix `:idx` where `idx` specifies which ancestor with that name to use (`1` = the most recent, `2` = the 2nd most recent, etc.) For example, consider the following:

```py
order_info = Orders.CALCULATE(y=YEAR(order_date), m=MONTH(order_date))
p1 = order_info.PARTITION(name="groups", by=(y, m))
p2 = p1.(name="groups", by=(y))
data = p2.groups.Orders

# Ranks each order per year/month by its total price.
# The full ancestry is p2 [name=groups] -> p1 [name=groups] -> order_info [name=Orders],
# So "groups:1" means the window function should be computed with regards to p1
# since it is the most recent ancestor with the name "groups".
data.CALCULATE(r=RANKING(by=total_price.DESC(), per="groups:1"))

# Ranks each order per year by its total price.
# The full ancestry is p2 [name=groups] -> p1 [name=groups] -> order_info [name=Orders],
# So "groups:2" means the window function should be computed with regards to p2
# since it is the 2nd most recent ancestor with the name "groups".
data.CALCULATE(r=RANKING(by=total_price.DESC(), per="groups:2"))
```

Below is each window function currently supported in PyDough.

<!-- TOC --><a name="ranking"></a>

### RANKING

The `RANKING` function returns ordinal position of the current record when all records in the current context are sorted by certain ordering keys. The arguments:

- `by`:1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. PyDough provides `collation_default_asc` and `propagate_collation` configs to control the default collation and whether to propagate the collation if the current expression is not a collation expression. Please see the [Session Configs](./usage.md#session-configs) documentation for more details.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `allow_ties` (optional): optional argument (default False) specifying to allow values that are tied according to the `by` expressions to have the same rank value. If False, tied values have different rank values where ties are broken arbitrarily.
- `dense` (optional): optional argument (default False) specifying that if `allow_ties` is True and a tie is found, should the next value after the ties be the current ranking value plus 1, as opposed to jumping to a higher value based on the number of ties that were there. For example, with the values `[a, a, b, b, b, c]`, the values with `dense=True` would be `[1, 1, 2, 2, 2, 3]`, but with `dense=False` they would be `[1, 1, 3, 3, 3, 6]`.
- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. PyDough provides `collation_default_asc` and `propagate_collation` configs to control the default collation and whether to propagate the collation if the current expression is not a collation expression. Please see the [Session Configs](./usage.md#session-configs) documentation for more details.

```py
# Rank customers per-nation by their account balance
# (highest = rank #1, no ties)
Nations.customers.CALCULATE(r = RANKING(by=acctbal.DESC(), per="Nations"))

# For every customer, finds their most recent order
# (ties allowed)
customers.orders.WHERE(RANKING(by=order_date.DESC(), per="customers", allow_ties=True) == 1)
```

<!-- TOC --><a name="percentile"></a>

### PERCENTILE

The `PERCENTILE` function returns what index the current record belongs to if all records in the current context are ordered then split into evenly sized buckets. The arguments:

- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. PyDough provides `collation_default_asc` and `propagate_collation` configs to control the default collation and whether to propagate the collation if the current expression is not a collation expression. Please see the [Session Configs](./usage.md#session-configs) documentation for more details.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `n_buckets` (optional): optional argument (default 100) specifying the number of buckets to use. The first values according to the sort order are assigned bucket `1`, and the last values are assigned bucket `n_buckets`.

```py
# Keep the top 0.1% of customers with the highest account balances.
customers.WHERE(PERCENTILE(by=acctbal.ASC(), n_buckets=1000) == 1000)

# For every region, find the top 5% of customers with the highest account balances.
Regions.nations.customers.WHERE(PERCENTILE(by=acctbal.ASC(), per="Regions") > 95)
```

<!-- TOC --><a name="prev"></a>

### PREV

The `PREV` function returns the value of an expression from a preceding record in the collection. The arguments:

- `expression`: the expression to return the shifted value of.
- `n` (optional): optional argument (default `1`) how many records backwards to look.
- `default` (optional): optional argument (default `None`) the value to output when there is no record `n` before the current record. This must be a valid literal.
- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.

```py
# Find the 10 customers with at least 5 orders with the largest average time
# gap between their orders, in days.
order_info = orders.CALCULATE(
   day_diff=DATEDIFF("days", PREV(order_date, by=order_date.ASC(), per="customers"), order_date)
)
customers.WHERE(COUNT(orders) > 5).CALCULATE(
   name,
   average_order_gap=AVG(order_info.day_diff)
).TOP_K(10, by=average_order_gap.DESC())

# For every year/month, calculate the percent change in the number of
# orders made in that month from the previous month.
PARTITION(
   Orders(year=YEAR(order_date), month=MONTH(order_date)),
   name="orders",
   by=(year, month)
).CALCULATE(
   year,
   month,
   n_orders=COUNT(orders),
   pct_change=
      100.0
      * (COUNT(orders) - PREV(COUNT(orders), by=(year.ASC(), month.ASC())))
      / PREV(COUNT(orders), by=(year.ASC(), month.ASC()))
)
```

<!-- TOC --><a name="next"></a>

### NEXT

The `NEXT` function returns the value of an expression from a following record in the collection. In other words, `NEXT(expr, n)` is the same as `PREV(expr, -n)`. The arguments:

- `expression`: the expression to return the shifted value of.
- `n` (optional): optional argument (default `1`) how many records forward to look.
- `default` (optional): optional argument (default `None`) the value to output when there is no record `n` after the current record. This must be a valid literal.
- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.


<!-- TOC --><a name="relsum"></a>

### RELSUM

The `RELSUM` function returns the sum of multiple rows of a singular expression within the same collection, e.g. the global sum across all rows, or the sum of rows per an ancestor of a sub-collection. The arguments:

- `expression`: the singular expression to take the sum of across multiple rows.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `by` (optional): 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. Can only be provided if `cumulative` is True.
- `cumulative` (optional): optional argument (default `False`) that can only be `True` if the `by` argument is provided. If `True`, then instead of returning the sum of all of the data in the context, returns the cumulative sum of all rows up to and including the current row when sorted according to the keys in the `by` argument.

For example:

```py
# Finds the ratio between each customer's account balance and the global
# sum of all customers' account balances.
customers.CALCULATE(ratio=acctbal / RELSUM(acctbal))

# Finds the ratio between each customer's account balance and the sum of all
# all customers' account balances within that nation.
Nations.customers.CALCULATE(ratio=acctbal / RELSUM(acctbal, per="Nations"))

# Finds, for each customer, the wealth of that customer combined with all
# poorer customers.
Customers.CALCULATE(cumulative_wealth=RELSUM(acctbal, by=acctbal.ASC(), cumulative=True))
```


<!-- TOC --><a name="relavg"></a>

### RELAVG

The `RELAVG` function returns the average of multiple rows of a singular expression within the same collection, e.g. the global average across all rows, or the average of rows per an ancestor of a sub-collection. The arguments:

- `expression`: the singular expression to take the average of across multiple rows.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `by` (optional): 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. Can only be provided if `cumulative` is True.
- `cumulative` (optional): optional argument (default `False`) that can only be `True` if the `by` argument is provided. If `True`, then instead of returning the average of all of the data in the context, returns the cumulative average of all rows up to and including the current row when sorted according to the keys in the `by` argument.

```py
# Finds all customers whose account balance is above the global average of all
# customers' account balances.
customers.WHERE(acctbal > RELAVG(acctbal))

# Finds all customers whose account balance is above the average of all
# customers' account balances within that nation.
Nations.customers.WHERE(acctbal > RELAVG(acctbal, per="Nations"))

# Finds the cumulative average of the total price of orders when sorted by
# order date (breaking ties by the order key).
Orders.CALCULATE(average_price_so_far=RELAVG(total_price, by=(order_date.ASC(), key.ASC()), cumulative=True))
```


<!-- TOC --><a name="relcount"></a>

### RELCOUNT

The `RELCOUNT` function returns the number of non-null records in multiple rows of a singular expression within the same collection, e.g. the count of all non-null rows, or the number of non-null rows per an ancestor of a sub-collection. The arguments:

- `expression`: the singular expression to count the number of non-null entries across multiple rows.
- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `by` (optional): 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. Can only be provided if `cumulative` is True.
- `cumulative` (optional): optional argument (default `False`) that can only be `True` if the `by` argument is provided. If `True`, then instead of returning the count of all of the data in the context, returns the cumulative count of all rows up to and including the current row when sorted according to the keys in the `by` argument.


```py
# Divides each customer's account balance by the total number of positive
# account balances globally.
customers.CALCULATE(ratio = acctbal / RELCOUNT(KEEP_IF(acctbal, acctbal > 0.0)))

# Divides each customer's account balance by the total number of positive
# account balances in the same nation.
Nations.customers.CALCULATE(ratio = acctbal / RELCOUNT(KEEP_IF(acctbal, acctbal > 0.0), per="Nations"))

# For each customer, count how many customers are poorer than them but are not
# in debt.
Customers.CALCULATE(n_poorer_non_debt=RELCOUNT(KEEP_IF(acctbal, acctbal >= 0), by=(acctbal.ASC()), cumulative=True) - (acctbal >= 0))
```


<!-- TOC --><a name="relsize"></a>

### RELSIZE

The `RELSIZE` function returns the number of total records, either globally or the number of sub-collection rows per some ancestor collection. The arguments:

- `per` (optional): optional argument (default `None`) for the same `per` argument as all other window functions.
- `by` (optional): 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context. Can only be provided if `cumulative` is True.
- `cumulative` (optional): optional argument (default `False`) that can only be `True` if the `by` argument is provided. If `True`, then instead of returning the number rows in the context, returns the cumulative number of rows up to and including the current row when sorted according to the keys in the `by` argument.


```py
# Divides each customer's account balance by
# the number of total customers.
customers.CALCULATE(ratio = acctbal / RELSIZE())

# Divides each customer's account balance by the
# number of total customers in that nation.
Nations.customers.CALCULATE(ratio = acctbal / RELSIZE(per="Nations"))

# For each customer, returns the number of customers poorer than them.
Customers.CALCULATE(customers_poorer=RELSIZE(by=(acctbal.ASC()), cumulative=True) - 1)
```


<!-- TOC --><a name="casting-functions"></a>

## Casting Functions

<!-- TOC --><a name="string"></a>
### STRING

The `STRING` function casts the first argument to a string data type. The first argument can be of any data type. This function also supports formatting of dates. This is possible by placing a date-time format string in the second argument. **Please refer to your underlying database's documentation for the format strings it supports.**


```py
Orders.CALCULATE(
   # Casts the key column (numeric type) to a string.
   key_string=STRING(key),
   # Casts the order_date column (date type) to a string
   # with the format YYYY-MM-DD.
   # Please refer to your underlying database's documentation
   # for the format strings it supports.
   # In this case, the database used is SQLite.
   order_date_string=STRING(order_date, "%Y-%m-%d"),
)
```

Here is a list of reference links for the format strings of different databases:

- [SQLite](https://www.sqlite.org/lang_datefunc.html)

<!-- TOC --><a name="integer"></a>

### INTEGER

The `INTEGER` function casts the argument to an integer.

```py
Orders.CALCULATE(
   # Casts the total_price column (decimal type) to an integer.
   total_price_int=INTEGER(total_price),
   # Casts the string "2" to an integer.
   discount = INTEGER("2")
)
```

<!-- TOC --><a name="float"></a>

### FLOAT

The `FLOAT` function casts the argument to a float.

```py
Orders.CALCULATE(
   # Casts the ship_priority column (integer type) to a float.
   ship_priority_float=FLOAT(ship_priority),
   # Casts the string "2" to a float.
   discount = FLOAT("-2.71")
)
```

## Banned Python Logic

Below is a list of banned python logic (magic methods,etc.) that are not supported in PyDough. Calling these methods will result in an Exception.

<!-- TOC --><a name="__bool__"></a>

### \_\_bool\_\_

The `__bool__` magic method is not supported in PyDough. PyDough code cannot be treated as booleans.

```py
# Not allowed - will raise PyDoughUnqualifiedException
if Customer and Order:
   print("Available")

customers.WHERE((acctbal > 0) and (nation.name == "GERMANY"))
# Use &`instead of `and`:
# customers.WHERE((acctbal > 0) & (nation.name == "GERMANY"))

Orders.WHERE((discount > 0.05) or (tax > 0.08))
# Use `|` instead of `or` 
# Orders.WHERE((discount > 0.05) | (tax > 0.08))

Parts.WHERE(not(retail_price > 1000))
# Use `~` instead of `not`
# Parts.WHERE(~(retail_price > 1000))
```

<!-- TOC --><a name="__call__"></a>

### \_\_call\_\_

The `__call__` magic method is not supported in PyDough as it calls PyDough code as if it were a function.

```py
# Not allowed - calls PyDough code as if it were a function
(1 - discount)(extended_price * 0.5)
```

<!-- TOC --><a name="__floor__"></a>

### \_\_floor\_\_

The `math.floor` function calls the `__floor__` magic method, which is currently not supported in PyDough.

```py
Customer(age=math.floor(order.total_price))
```

<!-- TOC --><a name="__ceil__"></a>

### \_\_ceil\_\_

The `math.ceil` function calls the `__ceil__` magic method, which is currently not supported in PyDough.

```py
# Not allowed currently- will raise PyDoughUnqualifiedException
Customer(age=math.ceil(order.total_price))
```

<!-- TOC --><a name="__trunc__"></a>

### \_\_trunc\_\_

The `math.trunc` function calls the `__trunc__` magic method, which is currently not supported in PyDough.

```py
# Not allowed currently- will raise PyDoughUnqualifiedException
Customer(age=math.trunc(order.total_price))
```

<!-- TOC --><a name="__reversed__"></a>

### \_\_reversed\_\_

The `reversed` function calls the `__reversed__` magic method, which is currently not supported in PyDough.

```py
# Not allowed currently- will raise PyDoughUnqualifiedException
Regions(backwards_name=reversed(name))
```

<!-- TOC --><a name="__int__"></a>

### \_\_int\_\_

Casting to `int` calls the `__int__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return an integer instead of a PyDough object.

```py
# Not allowed currently as it would need to return an int instead of a PyDough object
Orders(limit=int(order.total_price))
```

<!-- TOC --><a name="__float__"></a>

### \_\_float\_\_

Casting to `float` calls the `__float__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return a float instead of a PyDough object.

```py
# Not allowed currently as it would need to return a float instead of a PyDough object
Orders(limit=float(order.quantity))
```

<!-- TOC --><a name="__complex__"></a>

### \_\_complex\_\_

Casting to `complex` calls the `__complex__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return a complex instead of a PyDough object.

```py
# Not allowed currently as it would need to return a complex instead of a PyDough object
Orders(limit=complex(order.total_price))
```

<!-- TOC --><a name="__index__"></a>

### \_\_index\_\_

Using an object as an index calls the `__index__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return an integer instead of a PyDough object.

```py
# Not allowed currently as it would need to return an int instead of a PyDough object
Orders(s="ABCDE"[:order_priority])
```

<!-- TOC --><a name="__nonzero__"></a>

### \_\_nonzero\_\_

Using an object in a boolean context calls the `__nonzero__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return an integer instead of a PyDough object.

```py
# Not allowed currently as it would need to return an int instead of a PyDough object
Lineitems(is_taxed=bool(tax))
```

<!-- TOC --><a name="__len__"></a>

### \_\_len\_\_

The `len` function calls the `__len__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return an integer instead of a PyDough object. Instead, usage of [LENGTH](#length) function is recommended.

```py
# Not allowed currently as it would need to return an int instead of a PyDough object
customers(len(customer.name))
```

<!-- TOC --><a name="__contains__"></a>

### \_\_contains\_\_

Using the `in` operator calls the `__contains__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return a boolean instead of a PyDough object. Instead, If you need to check if a string is inside another substring, use [CONTAINS](#contains). If you need to check if an expression is a member of a list of literals, use [ISIN](#isin).

```py
# Not allowed currently as it would need to return a boolean instead of a PyDough object
Orders('discount' in order.details)
```

<!-- TOC --><a name="__setitem__"></a>

### \_\_setitem\_\_

Assigning to an index calls the `__setitem__` magic method, which is not supported in PyDough. This operation is not allowed.

```py
# Not allowed currently as PyDough objects cannot support item assignment.
Order.details['discount'] = True
```

<!-- TOC --><a name="__iter__"></a>

### \_\_iter\_\_

Iterating over an object calls the `__iter__` magic method, which is not supported in PyDough. This operation is not allowed because the implementation has to return an iterator instead of a PyDough object.

```py
# Not allowed currently as implementation has to return an iterator instead of a PyDough object.
for item in customer:
   print(item)

[item for item in customer]

list(customer)

tuple(customer)
```
