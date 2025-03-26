# Configs

This subdirectory of the PyDough directory deals with the configuration settings for PyDough.

The configs module provides functionality to manage various configuration settings that affect the behavior of PyDough.

## Available APIs

### [pydough_configs.py](pydough_configs.py)

- `PyDoughConfigs`: Class used to store information about various configuration settings of PyDough.
    - `sum_default_zero`: If True, the `SUM` function always defaults to 0 if there are no records to be summed up. If False, the output could be `NULL`. The default is True.
    - `avg_default_zero`: If True, the `AVG` function always defaults to 0 if there are no records to be averaged. If False, the output could be `NULL`. The default is False.
    - `collation_default_asc`: If True, the default collation is ascending. If False, the default collation is descending. The default is True.
    - `propogate_collation`: If True, the collation of the current expression, which does not have a collation, uses the most recent available collation in the nodes of the term. If False, the expression uses the default collation as specified by `collation_default_asc`. The default is False.
    - `start_of_week`: The day of the week that is considered the start of the week. The default is `DayOfWeek.SUNDAY`.
    - `start_week_as_zero`: If True, then the first day of the week is considered to be 0. If False, then the first day of the week is considered to be 1. The default is True.
- `DayOfWeek`: Enum to represent the day of the week.
    - `SUNDAY`: Sunday.
    - `MONDAY`: Monday.
    - `TUESDAY`: Tuesday.
    - `WEDNESDAY`: Wednesday.
    - `THURSDAY`: Thursday.
    - `FRIDAY`: Friday.
    - `SATURDAY`: Saturday.

### [session.py](session.py)

- `PyDoughSession`: Container class used to define a PyDough session. This includes both a set of properties that can be accessed and modified directly, as well as helper methods to assist with some of the plumbing.
    - `metadata`: Property to get or set the active metadata graph.
    - `config`: Property to get or set the active PyDough configuration.
    - `database`: Property to get or set the active database context.
    - `connect_database`: Method to create a new DatabaseContext and register it in the session.
    - `load_metadata_graph`: Method to load a metadata graph from a file and register it in the session.

## Active Session

PyDough maintains an active session that is used to process any user code. The active session can be accessed and modified via the `pydough.active_session` property. By default, most users will modify the active session, but it is also possible to swap out the active session for a new one if needed.

## Usage

To use the configs module, you can import the necessary classes and call them with the appropriate arguments. For example:

```python
from pydough.configs import PyDoughConfigs, PyDoughSession
import pydough

# Create a new configuration object and change its value of the
# `sum_default_zero` configuration
config = PyDoughConfigs()
config.sum_default_zero = False

# Create a new session and set the configuration
session = PyDoughSession()
session.config = config

# Connect to a database
session.connect_database("sqlite", database="path/to/database.db")

# Load a metadata graph
session.load_metadata_graph("path/to/graph.json", "graph_name")

# Access the active session
active_session = pydough.active_session

# Modify the active session's configuration
active_session.config.sum_default_zero = True
```
