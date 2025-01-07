# PyDough Operators

This module defines the operators used in PyDough for various operations such as arithmetic, logical, and aggregation functions. The operators are registered and made available for use in PyDough code.

## Available APIs

The PyDough operators module provides the following notable APIs:

- `builtin_registered_operators`: A function that returns a dictionary of all built-in registered operators.

## Usage

The registered operators are used in PyDough code without needing to directly import them. Instead, they are automatically available for use in PyDough expressions. For example:

### Arithmetic Operators

You can use arithmetic operators such as `+`, `-`, `*`, `/`, `%`, and `**` directly in PyDough expressions:

```python
result = extended_price * (1 - discount)
```

### Logical Operators

You can use logical operators such as `&`, `|`, and `^` directly in PyDough expressions:

```python
result = (region.name == "ASIA") & (COUNT(suppliers) > 100)
```

### Aggregation Functions

You can use aggregation functions such as `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` directly in PyDough expressions:

```python
result = COUNT(customers)
```

### Other Functions

You can use other functions such as `LOWER`, `UPPER`, `IFF`, and `CONTAINS` directly in PyDough expressions:

```python
result = LOWER(name)
```

### Window Functions

A window function returns information about the current record relative to its place amongst other records. This could include its ordinal position when sorted globally (or within other subcollection records of a parent collection).

```python
result = RANKING(by=account_balance.DESC())
```

## Detailed Explanation

The PyDough operators module defines various operators used in PyDough for different types of operations. These operators are registered and made available for use in PyDough code through the `builtin_registered_operators` function. This function returns a dictionary of all built-in registered operators, which are then automatically available for use in PyDough expressions.

By using the registered operators, users can write PyDough code that performs arithmetic, logical, and aggregation operations without needing to directly import the operators. This makes the code more concise and easier to read.

The operators module provides a flexible and extensible way to define and use operators in PyDough, allowing users to perform a wide range of operations in their data analysis and exploration tasks.
