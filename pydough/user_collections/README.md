# PyDough User Collections

This module defines the user collections that can be created on the fly and used in PyDough with other collections, for example: range collections, Pandas DataFrame collections. The user collections are registered and made available for use in PyDough code.

## Available APIs

### [range_collection.py](range_collection.py)

  - `RangeGeneratedCollection`: Class used to create a range collection that generates a sequence of numbers based on the specified start, end, and step values.
    - `name`: The name of the range collection.
    - `column_name`: The name of the column in the range collection.
    - `start`: The starting value of the range (inclusive).
    - `end`: The ending value of the range (exclusive).
    - `step`: The step value for incrementing the range. Default is 1.

### [user_collection_apis.py](user_collection_apis.py)
  - `range_collection`: Function to create a range collection with the specified parameters.
    - `name`: The name of the range collection.
    - `column_name`: The name of the column in the range collection.
    - `start`: The starting value of the range (inclusive).
    - `end`: The ending value of the range (exclusive).
    - `step`: The step value for incrementing the range. Default is 1.
    - Returns: An instance of `RangeGeneratedCollection`.

### [user_collections.py](user_collections.py)
  - `PyDoughUserGeneratedCollection`: Base class for all user-generated collections in PyDough.

## Usage

You can access user collections through `pydough` and call them with the required arguments. For example:

```python
import pydough

my_range = pydough.range_collection(
        "simple_range",
        "col1",
        1, 10, 2
    )
```
Output:
```
    col1
0     1
1     3
2     5
3     7
4     9
```

## Detailed Explanation

The user collections module provides a way to create collections that are not part of the static metadata graph but can be generated dynamically based on user input or code. The most common user collection are integer range collections and Pandas DataFrame collections.
The range collection, generates a sequence of numbers. The `RangeGeneratedCollection` class allows users to define a range collection by specifying the start, end, and step values. The `range_collection` function is a convenient API to create instances of `RangeGeneratedCollection`.