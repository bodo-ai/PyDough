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

### [dataframe_collection.py](dataframe_collection.py)

  - `DataframeGeneratedCollection`: Class used to create a dataframe collection using the given dataframe and name.
    - `name`: The name of the dataframe collection.
    - `dataframe`: The Pandas dataframe containing all data (rows and columns).
    - `unique_column_names`: List of strings or list of list of string 
    `(list [str | list[ str ]])` representing the unique properties for the dataframe 
    collection. For example: ["column1", ["column2", "column3"]] indicates `column1`
    is a unique property and the combination of column2 and column3 is also unique.
    - `column_subset`(optional): List of filter/selected columns from the dataframe.
    If provided, indicates all columns from the original dataframe that will be in the
    final dataframe collection. 

    **Note**: All columns in `unique_column_names` must be included in `column_subset`; otherwise, an error will be raised. 

### [user_collection_apis.py](user_collection_apis.py)
  - `range_collection`: Function to create a range collection with the specified parameters.
    - `name`: The name of the range collection.
    - `column_name`: The name of the column in the range collection.
    - `start`: The starting value of the range (inclusive).
    - `end`: The ending value of the range (exclusive).
    - `step`: The step value for incrementing the range. Default is 1.
    - Returns: An instance of `RangeGeneratedCollection`.
  - `dataframe_collection`: Function to create a dataframe collection with the specified parameters.
    - `name`: The name of the dataframe collection.
    - `dataframe`: The Pandas dataframe.
    - `unique_column_names`: List of unique columns or unique combination of columns.
    - `column_subset`(optional): List of columns filtered from the given dataframe.
    - Returns: An instance of `DataframeGeneratedCollection`.


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

Dataframe collection example:
```python
import pydough
import pandas as pd

df = pd.DataFrame({
  "color": ["red", "orange", "yellow", "green", "blue", "indigo", "violet", None]
  "idx": range(8)
})
rainbow_table = pydough.dataframe_collection(name='rainbow', dataframe=df)
df = pydough.to_df(rainbow_table)
print(df)
```
Output:
```
    color   idx
0   red     1
1   orange  2
2   yellow  3
3   green   4
4   blue    5
5   indigo  6
6   violet  7
7   None    8
```

## Detailed Explanation

The user collections module provides a way to create collections that are not part of the static metadata graph but can be generated dynamically based on user input or code. The most common user collection are integer range collections and Pandas DataFrame collections.
The range collection, generates a sequence of numbers. The `RangeGeneratedCollection` class allows users to define a range collection by specifying the start, end, and step values. The `range_collection` function is a convenient API to create instances of `RangeGeneratedCollection`.
The dataframe collection, generates a collection based on the given Pandas Dataframe. The `DataframeGeneratedCollection` class
allows user to create a collection by specifying the dataframe and name. The `dataframe_collection` function is a convenient API
to create instances of `DataframeGeneratedCollection`.