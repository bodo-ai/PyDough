# PyDough Functions List

Below is the list of every function/operator currently supported in PyDough as a builtin.

## Binary Operators

Below is each binary operator currently supported in PyDough.

### Arithmetic

Numerical expression values can be:
- Added together with the `+` operator
- Subtracted from one another with the `-` operator
- Multiplied by one another with the `*` operator
- divided by one another with the `/` operator (note: the behavior when the denominator is `0` depends on the database being used to evaluate the expression)

```py
Lineitems(value = (extended_price * (1 - discount) + 1.0) / part.retail_price)
```

### Comparisons

Expression values can be compared to one another with the standard comparison operators `<=`, `<`, `==`, `!=`, `>` and `>=`:

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
> Do **NOT** use use chained inequalities like `a <= b <= c`, as this can cause undefined incorrect behavior in PyDough. Instead, use expressions like `(a <= b) & (b <= c)`.

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

## Unary Operators

Below is each unary operator currently supported in PyDough.

### Negation

A numerical expression can have its sign flipped by prefixing it with the `-` operator:

```py
Lineitems(lost_value = extended_price * (-discount))
```

## Other Operators

Below are all other operators currently supported in PyDough that use other syntax besides function calls:

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

## String Functions

Below is each function currently supported in PyDough that operates on strings.

### LOWER

Calling `LOWER` on a string converts its characters to lowercase:

```py
Customers(lowercase_name = LOWER(name))
```

### UPPER

Calling `UPPER` on a string converts its characters to uppercase:

```py
Customers(uppercase_name = UPPER(name))
```

### STARTSWITH

The `STARTSWITH` function returns whether its first argument begins with its second argument as a string prefix:

```py
Parts(begins_with_yellow = STARTSWITH(name, "yellow"))
```

### ENDSWITH

The `ENDSWITH` function returns whether its first argument ends with its second argument as a string suffix:

```py
Parts(ends_with_chocolate = ENDSWITH(name, "chocolate"))
```

### CONTAINS

The `CONTAINS` function returns whether its first argument contains with its second argument as a substring:

```py
Parts(is_green = CONTAINS(name, "green"))
```

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
