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
   * [STARTSWITH](#startswith)
   * [ENDSWITH](#endswith)
   * [CONTAINS](#contains)
   * [LIKE](#like)
   * [JOIN_STRINGS](#join_strings)
- [Datetime Functions](#datetime-functions)
   * [YEAR](#year)
   * [MONTH](#month)
   * [DAY](#day)
- [Conditional Functions](#conditional-functions)
   * [IFF](#iff)
   * [DEFAULT_TO](#default_to)
   * [PRESENT](#present)
   * [ABSENT](#absent)
   * [KEEP_IF](#keep_if)
   * [MONOTONIC](#monotonic)
- [Numerical Functions](#numerical-functions)
   * [ABS](#abs)
   * [ROUND](#round)
- [Aggregation Functions](#aggregation-functions)
   * [SUM](#sum)
   * [MIN](#min)
   * [MAX](#max)
   * [COUNT](#count)
   * [NDISTINCT](#ndistinct)
   * [HAS](#has)
   * [HASNOT](#hasnot)
- [Window Functions](#window-functions)
   * [RANKING](#ranking)
   * [PERCENTILE](#percentile)

<!-- TOC end -->

<!-- TOC --><a name="binary-operators"></a>
## Binary Operators

Below is each binary operator currently supported in PyDough.

<!-- TOC --><a name="arithmetic"></a>
### Arithmetic

Numerical expression values can be:
- Added together with the `+` operator
- Subtracted from one another with the `-` operator
- Multiplied by one another with the `*` operator
- divided by one another with the `/` operator (note: the behavior when the denominator is `0` depends on the database being used to evaluate the expression)

```py
Lineitems(value = (extended_price * (1 - discount) + 1.0) / part.retail_price)
```

<!-- TOC --><a name="comparisons"></a>
### Comparisons

Expression values can be compared using standard comparison operators: `<=`, `<`, `==`, `!=`, `>` and `>=`:

```py
Customers(
    in_debt = acctbal < 0,
    at_most_12_orders = COUNT(orders) <= 12,
    is_european = nation.region.name == "EUROPE",
    non_german = nation.name != "GERMANY",
    non_empty_acct = acctbal > 0,
    at_least_5_orders = COUNT(orders) >= 5,
)
```

> [!WARNING]
> Chained inequalities, like `a <= b <= c`, can cause undefined/incorrect behavior in PyDough. Instead, use expressions like `(a <= b) & (b <= c)`.

<!-- TOC --><a name="logical"></a>
### Logical

Multiple boolean expression values can be logically combined with `&`, `|` and `~` being used as logical AND, OR and NOT, respectively:

```py
is_asian = nation.region.name == "ASIA"
is_european = nation.region.name == "EUROPE"
in_debt = acctbal < 0
Customers(
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
Lineitems(lost_value = extended_price * (-discount))
```

<!-- TOC --><a name="other-operators"></a>
## Other Operators

Below are all other operators currently supported in PyDough that use other syntax besides function calls:

<!-- TOC --><a name="slicing"></a>
### Slicing

A string expression can have a substring extracted with Python string slicing syntax `s[a:b:c]`:

```py
Customers(
    country_code = phone[:3],
    name_without_first_char = name[1:]
)
```

> [!WARNING]
> PyDough currently only supports combinations of `string[start:stop:step]` where `step` is either 1 or missing, and where both `start` and `stop` are either non-negative values or are missing.

<!-- TOC --><a name="string-functions"></a>
## String Functions

Below is each function currently supported in PyDough that operates on strings.

<!-- TOC --><a name="lower"></a>
### LOWER

Calling `LOWER` on a string converts its characters to lowercase:

```py
Customers(lowercase_name = LOWER(name))
```

<!-- TOC --><a name="upper"></a>
### UPPER

Calling `UPPER` on a string converts its characters to uppercase:

```py
Customers(uppercase_name = UPPER(name))
```

<!-- TOC --><a name="startswith"></a>
### STARTSWITH

The `STARTSWITH` function returns whether its first argument begins with its second argument as a string prefix:

```py
Parts(begins_with_yellow = STARTSWITH(name, "yellow"))
```

<!-- TOC --><a name="endswith"></a>
### ENDSWITH

The `ENDSWITH` function returns whether its first argument ends with its second argument as a string suffix:

```py
Parts(ends_with_chocolate = ENDSWITH(name, "chocolate"))
```

<!-- TOC --><a name="contains"></a>
### CONTAINS

The `CONTAINS` function returns whether its first argument contains with its second argument as a substring:

```py
Parts(is_green = CONTAINS(name, "green"))
```

<!-- TOC --><a name="like"></a>
### LIKE

The `LIKE` function returns whether the first argument matches the SQL pattern text of the second argument, where `_` is a 1 character wildcard and `%` is an 0+ character wildcard.

```py
Orders(is_special_request = LIKE(comment, "%special%requests%"))
```

Below are some examples of how to interpret these patterns:
- `"a_c"` returns True for any 3-letter string where the first character is `"a"` and the third is `"c"`.
- `"_q__"` returns True for any 4-letter string where the second character is `"q"`.
- `"%_s"` returns True for any 2+-letter string where the last character is `"s"`.
- `"a%z"` returns True for any string that starts with `"a"` and ends with `"z"`.
- `"%a%z%"` returns True for any string that contains an `"a"`, and also contains a `"z"` at some later point in the string.
- `"_e%"` returns True for any string where the second character is `"e"`.

<!-- TOC --><a name="join_strings"></a>
### JOIN_STRINGS

The `JOIN_STRINGS` function combines all of its string arguments by concatenating every argument after the first argument, using the first argument as a delimiter between each of the following arguments (like the `.join` method in Python):

```py
Regions.nations.customers(
    fully_qualified_name = JOIN_STRINGS("-", BACK(2).name, BACK(1).name, name)
)
```

For instance, `JOIN_STRINGS("; ", "Alpha", "Beta", "Gamma)` returns `"Alpha; Beta; Gamma"`.

<!-- TOC --><a name="datetime-functions"></a>
## Datetime Functions

Below is each function currently supported in PyDough that operates on date/time/timestamp values.

<!-- TOC --><a name="year"></a>
### YEAR

Calling `YEAR` on a date/timestamp extracts the year it belongs to:

```py
Orders.WHERE(YEAR(order_date) == 1995)
```

<!-- TOC --><a name="month"></a>
### MONTH

Calling `MONTH` on a date/timestamp extracts the month of the year it belongs to:

```py
Orders(is_summer = (MONTH(order_date) >= 6) & (MONTH(order_date) <= 8))
```

<!-- TOC --><a name="day"></a>
### DAY

Calling `DAY` on a date/timestamp extracts the day of the month it belongs to:

```py
Orders(is_first_of_month = DAY(order_date) == 1)
```

<!-- TOC --><a name="conditional-functions"></a>
## Conditional Functions

Below is each function currently supported in PyDough that handles conditional logic.

<!-- TOC --><a name="iff"></a>
### IFF

The `IFF` function cases on the True/False value of its first argument. If it is True, it returns the second argument, otherwise it returns the third argument. In this way, the PyDough code `IFF(a, b, c)` is semantically the same as the SQL expression `CASE WHEN a THEN b ELSE c END`.

```py
qty_from_germany = IFF(supplier.nation.name == "GERMANY", quantity, 0)
Customers(
    total_quantity_shipped_from_germany = SUM(lines(q=qty_from_germany).q)
)
```

<!-- TOC --><a name="default_to"></a>
### DEFAULT_TO

The `DEFAULT_TO` function returns the first of its arguments that is non-null (e.g. the same as the `COALESCE` function in SQL):

```py
Lineitems(adj_tax = DEFAULT_TO(tax, 0))
```

<!-- TOC --><a name="present"></a>
### PRESENT

The `PRESENT` function returns whether its argument is non-null (e.g. the same as `IS NOT NULL` in SQL):

```py
Lineitems(has_tax = PRESENT(tax))
```

<!-- TOC --><a name="absent"></a>
### ABSENT

The `ABSENT` function returns whether its argument is non-null (e.g. the same as `IS NULL` in SQL):

```py
Lineitems(no_tax = ABSENT(tax))
```

<!-- TOC --><a name="keep_if"></a>
### KEEP_IF

The `KEEP_IF` function returns the first function if the second arguments is True, otherwise it returns a null value. In other words, `KEEP_IF(a, b)` is equivalent to the SQL expression `CASE WHEN b THEN a END`.

```py
TPCH(avg_non_debt_balance = AVG(Customers(no_debt_bal = KEEP_IF(acctbal, acctbal > 0)).no_debt_bal))
```

<!-- TOC --><a name="monotonic"></a>
### MONOTONIC

The `MONOTONIC` function returns whether all of its arguments are in ascending order (e.g. `MONOTONIC(a, b, c, d)` is equivalent to `(a <= b) & (b <= c) & (c <= d)`):

```py
Lineitems.WHERE(MONOTONIC(10, quantity, 20) & MONOTONIC(5, part.size, 13))
```

<!-- TOC --><a name="numerical-functions"></a>
## Numerical Functions

Below is each numerical function currently supported in PyDough.

<!-- TOC --><a name="abs"></a>
### ABS

The `ABS` function returns the absolute value of its input.

```py
Customers(acct_magnitude = ABS(acctbal))
```

<!-- TOC --><a name="round"></a>
### ROUND

The `ROUND` function rounds its first argument to the precision of its second argument. The rounding rules used depend on the database's round function.

```py
Parts(rounded_price = ROUND(retail_price, 1))
```

<!-- TOC --><a name="aggregation-functions"></a>
## Aggregation Functions

Normally, functions in PyDough maintain the cardinality of their inputs. Aggregation functions instead take in an argument that can be plural and aggregates it into a singular value with regards to the current context. Below is each function currently supported in PyDough that can aggregate plural values into a singular value.

<!-- TOC --><a name="sum"></a>
### SUM

The `SUM` function returns the sum of the plural set of numerical values it is called on.

```py
Nations(total_consumer_wealth = SUM(customers.acctbal))
```

<!-- TOC --><a name="min"></a>
### MIN

The `MIN` function returns the smallest value from the set of numerical values it is called on.

```py
Suppliers(cheapest_part_supplied = MIN(supply_records.supply_cost))
```

<!-- TOC --><a name="max"></a>
### MAX

The `MAX` function returns the largest value from the set of numerical values it is called on.

```py
Suppliers(most_expensive_part_supplied = MIN(supply_records.supply_cost))
```

<!-- TOC --><a name="count"></a>
### COUNT

The `COUNT` function returns how many non-null records exist on the set of plural values it is called on.

```py
Customers(num_taxed_purchases = COUNT(orders.lines.tax))
```

The `COUNT` function can also be called on a sub-collection, in which case it will return how many records from that sub-collection exist.

```py
Nations(num_customers_in_debt = COUNT(customers.WHERE(acctbal < 0)))
```

<!-- TOC --><a name="ndistinct"></a>
### NDISTINCT

The `NDISTINCT` function returns how many distinct values of its argument exist.

```py
Customers(num_unique_parts_purchased = NDISTINCT(orders.lines.parts.key))
```

<!-- TOC --><a name="has"></a>
### HAS

The `HAS` function is called on a sub-collection and returns True if at least one record of the sub-collection exists. In other words, `HAS(x)` is equivalent to `COUNT(x) > 0`.

```py
Parts.WHERE(HAS(supply_records.supplier.WHERE(nation.name == "GERMANY")))
```

<!-- TOC --><a name="hasnot"></a>
### HASNOT

The `HASNOT` function is called on a sub-collection and returns True if no records of the sub-collection exist. In other words, `HASNOT(x)` is equivalent to `COUNT(x) == 0`.

```py
Customers.WHERE(HASNOT(orders))
```

<!-- TOC --><a name="window-functions"></a>
## Window Functions

Window functions are special functions that return a value for each record in the current context that depends on other records in the same context. A common example of this is ordering all values within the current context to return a value that depends on the current record's ordinal position relative to all the other records in the context.

Window functions in PyDough have an optional `levels` argument. If this argument is not provided, it means that the window function applies to all records of the current collection without any boundaries between records. If it is provided, it should be a value that can be used as an  argument to `BACK`, and in that case it means that the window function should be used on records of the current collection grouped by that particular ancestor.

For example, if using the `RANKING` window function, consider the following examples:

```py
# (no levels) rank every customer relative to all other customers
Regions.nations.customers(r=RANKING(...))

# (levels=1) rank every customer relative to other customers in the same nation
Regions.nations.customers(r=RANKING(..., levels=1))

# (levels=2) rank every customer relative to other customers in the same region
Regions.nations.customers(r=RANKING(..., levels=2))

# (levels=3) rank every customer relative to all other customers
Regions.nations.customers(r=RANKING(..., levels=3))
```

Below is each window function currently supported in PyDough.

<!-- TOC --><a name="ranking"></a>
### RANKING

The `RANKING` function returns ordinal position of the current record when all records in the current context are sorted by certain ordering keys. The arguments:

- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context.
- `levels`: same `levels` argument as all other window functions.
- `allow_ties`: optional argument (default False) specifying to allow values that are tied according to the `by` expressions to have the same rank value. If False, tied values have different rank values where ties are broken arbitrarily.
- `dense`: optional argument (default False) specifying that if `allow_ties` is True and a tie is found, should the next value after hte ties be the current ranking value plus 1, as opposed to jumping to a higher value based on the number of ties that were there. For example, with the values `[a, a, b, b, b, c]`, the values with `dense=True` would be `[1, 1, 2, 2, 2, 3]`, but with `dense=False` they would be `[1, 1, 3, 3, 3, 6]`.

```py
# Rank customers per-nation by their account balance
# (highest = rank #1, no ties)
Nations.customers(r = RANKING(by=acctbal.DESC(), levels=1))

# For every customer, finds their most recent order
# (ties allowed)
Customers.orders.WHERE(RANKING(by=order_date.DESC(), levels=1, allow_ties=True) == 1)
```

<!-- TOC --><a name="percentile"></a>
### PERCENTILE

The `PERCENTILE` function returns what index the current record belongs to if all records in the current context are ordered then split into evenly sized buckets. The arguments:

- `by`: 1+ collation values, either as a single expression or an iterable of expressions, used to order the records of the current context.
- `levels`: same `levels` argument as all other window functions.
- `n_buckets`: optional argument (default 100) specifying the number of buckets to use. The first values according to the sort order are assigned bucket `1`, and the last values are assigned bucket `n_buckets`.

```py
# Keep the top 0.1% of customers with the highest account balances.
Customers.WHERE(PERCENTILE(by=acctbal.ASC(), n_buckets=1000) == 1000)

# For every region, find the top 5% of customers with the highest account balances.
Regions.nations.customers.WHERE(PERCENTILE(by=acctbal.ASC(), levels=2) > 95)
```
