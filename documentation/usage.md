# PyDough Usage Guide

This document describes how to set up & interact with PyDough. For instructions on how to write PyDough code, see the [PyDough DSL](dsl.md) or the [list of PyDough builtin functions](functions.md).

<!-- TOC start (generated with https://github.com/derlin/bitdowntoc) -->

- [Setting Up in Jupyter Notebooks](#setting-up-in-jupyter-notebooks)
- [Session APIs](#session-apis)
   * [PyDough Session](#pydough-session)
   * [Session Configs](#session-configs)
   * [Session Database](#session-database)
   * [Session Knowledge Graph](#session-knowledge-graph)
- [Evaluation APIs](#evaluation-apis)
   * [`pydough.to_sql`](#pydoughto_sql)
   * [`pydough.to_df`](#pydoughto_df)
- [Transformation APIs](#transformation-apis)
   * [`pydough.from_string`](#pydoughfrom_string)
- [Exploration APIs](#exploration-apis)
   * [`pydough.explain_structure`](#pydoughexplain_structure)
   * [`pydough.explain`](#pydoughexplain)
   * [`pydough.explain_term`](#pydoughexplain_term)
- [Logging](#logging)

<!-- TOC end -->

<!-- TOC --><a name="setting-up-in-jupyter-notebooks"></a>
## Setting Up in Jupyter Notebooks

Once you have uv set up, you can run the command `uv run jupyter lab` from the PyDough directory to boot up a Jupyter lab process that will have access to PyDough. If you have installed PyDough via pip, you should be able to directly boot up a Jupyter lab process and it will have access to the PyDough module.

Once JupyterLab is running, you can either navigate to an existing notebook file or create a new one. In that notebook file, follow these steps in the notebook cells to work with PyDough:

1. Run `import pydough` to import the PyDough module.
2. Run `%load_ext pydough.jupyter_extensions` to load the PyDough Jupyter extension.
3. Connect to the metadata graph you wish to use. The metadata should be in a JSON file that can be accessed from the notebook. If the file is located at `<path>`, and the name of the graph within the file that you wish to use is `<graphname>`, then the connection command is `pydough.active_session.load_metadata_graph(<path>, <graphname>)`.
4. (optional) Connect to the database you wish to use to evaluate your analytical questions. If your database is sqlite and is located at `<path>`, the connection command is `pydough.active_session.connect_database("sqlite", database=<path>)`

To summarize, your notebook should start with a cell like this, which you need to run before proceeding with any other tasks:

```py
import pydough
%load_ext pydough.jupyter_extensions
pydough.active_session.load_metadata_graph("knowledge_graphs.json", "MyGraph")
pydough.active_session.connect_database("sqlite", "mydata.db")
```

Once you have done all of these steps, you can run PyDough in any cell of the notebook by prepending the cell with `%%pydough`. For example:

```py
%%pydough
result = nations.WHERE(region.name == "ASIA").CALCULATE(name, n_cust=COUNT(customers))
pydough.to_df(result)
```

Within the contents of a PyDough cell, the following is True:
- Any undefined variable is presumed to be either a function or an access to a collection/property described within the knowledge graph. This presumption is not checked until the code is fully evaluated by calling `pydough.to_df`.
    - For instance, in the example above, the following are all undefined variables that the PyDough cell resolves: `nations`, `region`, `name`, `COUNT`, `customers`.
- Any variables defined outside the current cell are still accessible inside the current cell.
- Any variables defined inside the cell are usable outside the cell.

<!-- TOC --><a name="session-apis"></a>
## Session APIs

This section describes various APIs you can use to interact with the PyDough session.

<!-- TOC --><a name="pydough-session"></a>
### PyDough Session

A `PyDoughSession` object encapsulates information about the knowledge graph being used, the configuration settings that have been enabled, the database connection being used, and any associated SQL dialect-specific settings.

A new `PyDoughSession` object can be created as follows:

```py
from pydough.configs import PyDoughSession
session = PyDoughSession()
```

There exists a special session object `pydough.active_session` used to describe the settings for PyDough by default, unless alternate values are passed in to specific APIs. The active session can be swapped out for another session to keep track of multiple at once:

```py
import pydough
from pydough.configs import PyDoughSession
# Create a new session
session_a = PyDoughSession()
# Capture the original active session
session_b = pydough.active_session
# Replace the active session
pydough.active_session = session_a
# Restore the original active session
pydough.active_session = session_b
```

Each `PyDoughSession` has the following notable fields, each of which can be accessed or switched out:
- `metadata`: the knowledge graph used by PyDough to identify collections/properties. By default, there is no knowledge graph until one is attached to the session. [See here](#session-knowledge-graph) for more details.
- `config`: the miscellaneous configurations used by the PyDough session. By default, each configuration is set to its default value. [See here](#session-configs) for more details.
- `database`: the database context used by PyDough to determine which version of SQL to translate into and used to execute queries in a database. By default, there is no database connection until one is attached to the session. [See here](#session-database) for more details.

<!-- TOC --><a name="session-knowledge-graph"></a>
### Session Knowledge Graph

The knowledge graph contains all of the metadata used by PyDough to identify every known collection and all of its properties, including both scalar attributes and sub-collection relationships. The metadata itself is stored in an ancillary file, which currently is only allowed to be a [JSON file in the supported format](metadata.md).

There are two ways to load a knowledge graph into a session:
1. Parse the graph with `parse_json_metadata_from_file` then set `session.graph` to the returned value.
2. Use `session.load_metadata_graph`.

Both examples are shown below. For these examples, assume there is a made-up JSON file located at `hello/world/graphs.json` and that the specific graph that should be used from this file is called `Food` (since the file could contain multiple graphs).

```py
import pydough
from pydough import parse_json_metadata_from_file

# First approach
my_graph = parse_json_metadata_from_file("hello/world/graphs.json", "Food")
pydough.active_session.graph = my_graph

# Second approach
my_graph = pydough.active_session.load_metadata_graph("hello/world/graphs.json", "Food")
```

Notice that both `parse_json_metadata_from_file` and `load_metadata_graph` also return the graph metadata. This is useful in case the graphs need to be switched around just like sessions. However, the graphs themselves are not intended to be examined by users except via [the exploration APIs](#exploration-apis), some of which require access to the graph object.

<!-- TOC --><a name="session-configs"></a>
### Session Configs

The miscellaneous configuration settings in PyDough are controlled by a `PyDoughConfigs` object. Each `PyDoughConfigs` contains several fields used at different points in the execution of PyDough to control ambiguous decisions where more than one option is valid. Each of these fields has a default value, but can also be manually controlled by a user replacing the value.

The configurations of a session object can be accessed with the `.config` field of the session object. A new configuration object can be created & subbed-in as follows:

```py
import pydough
from pydough.configs import PyDoughConfigs
# Create a new session
new_configs = PyDoughConfigs()
# Capture the original configs of the active session
old_configs = pydough.active_session.config
# Replace the configs of the active session
pydough.active_session.config = new_configs
# Restore the original configs of the active session
pydough.active_session.config = old_configs
```

Each `PyDoughConfigs` currently encapsulates the following configurations:
1. `sum_default_zero` (default=True): if True, then `SUM` will always return `0` instead of `NULL` when there are no records to be summed (e.g. summing over an empty sub-collection). If False, then the behavior will depend on the database being used to execute (though this nearly always means returning `NULL`).
2. `avg_default_zero` (default=False): if True, then `AVG` will always return `0` instead of `NULL` when there are no records to be summed (e.g. taking the average from an empty sub-collection). If False, then the behavior will depend on the database being used to execute (though this nearly always means returning `NULL`).

For example, consider this PyDough snippet:

```py
%%pydough
selected_customers = customers.WHERE(CONTAINS(name, '2468'))
result = Nations.CALCULATE(
    name,
    total_bal=SUM(selected_customers.acctbal),
    average_bal=AVG(selected_customers.acctbal),
)
pydough.to_df(result)
```

Not every nation has at least 1 customer with a name containing the substring `2468`. For these rows in the result, the value of `total_bal` and `average_bal` will depend on whether `sum_default_zero` and `avg_default_zero` are True or not. Below is an example of accessing & switching out these values.

```py
import pydough
# Capture the configs of the active session
configs = pydough.active_session.config
# Access the original value of sum_default_zero (which should be True)
old_sum_behavior = configs.sum_default_zero
# Toggle the value of sum_default_zero to False
configs.sum_default_zero = False
```

The following configs are used in the behavior of `PERCENTILE`, `RANKING`, `ORDER_BY` and `TOP_K` where a collation(`ASC` or `DESC`) is not explicitly specified:

3. `collation_default_asc` (default=True): if True, then the default collation is ascending. If False, then the default collation is descending.
4. `propagate_collation` (default=False): if True, then the collation of the current expression, which does not have a collation, uses the most recent available collation in the nodes of the term. If there is no recent available collation, then the default collation is used as specified by `collation_default_asc`. If False, the expression uses the default collation as specified by `collation_default_asc`.

For example, consider the following PyDough code:

```py
%%pydough

import pydough
# The configs of the active session
configs = pydough.active_session.config

# The collations are not explicitly specified for few of the terms.
Suppliers.ORDER_BY(
  COUNT(lines),
  nation.name.ASC(),
  COUNT(supply_records),
  account_balance.DESC(),
  key,
)

# Let's see the behavior of the terms with different configurations
# With the default settings (collation_default_asc=True and propagate_collation=False)
Suppliers.ORDER_BY(
  COUNT(lines).ASC(),
  nation.name.ASC(),
  COUNT(supply_records).ASC(),
  account_balance.DESC(),
  key.ASC(),
)

# With collation_default_asc=False and propagate_collation=True
configs.collation_default_asc = False
configs.propagate_collation = True
Suppliers.ORDER_BY(
  COUNT(lines).DESC(),
  nation.name.ASC(),
  COUNT(supply_records).DESC(),
  account_balance.DESC(),
  key.DESC(),
)

# With collation_default_asc=False and propagate_collation=False
configs.collation_default_asc = False
configs.propagate_collation = False
Suppliers.ORDER_BY(
  COUNT(lines).DESC(),
  nation.name.ASC(),
  COUNT(supply_records).DESC(),
  account_balance.DESC(),
  key.DESC(),
)

# With collation_default_asc=True and propagate_collation=True
configs.collation_default_asc = True
configs.propagate_collation = True
Suppliers.ORDER_BY(
  COUNT(lines).ASC(),
  nation.name.ASC(),
  COUNT(supply_records).ASC(),
  account_balance.DESC(),
  key.DESC(),
)
```

The following configs are used in the behavior of `DAYOFWEEK`, `DATETIME`, and `DATEDIFF`:

5. `start_of_week` (default=`DayOfWeek.SUNDAY`): Determines which day is considered the first day of the week. This affects the following functions:
- `DAYOFWEEK` : A function that returns the number of days since the start of the week. Start of week is relative to the `start_of_week` config.
- `DATETIME` : This function also supports the `start of week` unit, which is relative to the `start_of_week` config.
- `DATEDIFF` : This function also supports difference between two dates in terms of weeks, which is relative to the `start_of_week` config.
- `DATE_TRUNC` : This function also supports truncating a date to the start of the week, which is relative to the `start_of_week` config.

The value must be one of the following `DayOfWeek` enum values:

   - `DayOfWeek.SUNDAY` (default)
   - `DayOfWeek.MONDAY`  
   - `DayOfWeek.TUESDAY`
   - `DayOfWeek.WEDNESDAY`
   - `DayOfWeek.THURSDAY`
   - `DayOfWeek.FRIDAY`
   - `DayOfWeek.SATURDAY`

   The `DayOfWeek` enum is defined in the `pydough.configs` module.

**Note:** In Snowflake, PyDough does not automatically detect changes to `WEEK_START` session parameter. Please configure the `start_of_week` in your PyDough configurations.


6. `start_week_as_zero` (default=True): if True, then the first day of the week is considered to be 0. If False, then the first day of the week is considered to be 1. This config is used by  `DAYOFWEEK` function.

```py
import pydough
from pydough.configs import DayOfWeek

# The configs of the active session
configs = pydough.active_session.config

# Understanding the behavior of DAYOFWEEK
# Set the start of the week to Monday
configs.start_of_week = DayOfWeek.MONDAY
# Set start of week to be 1
configs.start_week_as_zero = False
# This would return dow = 4, as March 20, 2025 is a Thursday and is the 4th day 
# of the week when the start of week is considered to be Monday
TPCH.calculate(dow = DAYOFWEEK("2025-03-20")) # dow = 4
# If start_week_as_zero is set to True (configs.start_week_as_zero = True), 
# then DAYOFWEEK("2025-03-20") = 3
configs.start_week_as_zero = True
TPCH.calculate(dow = DAYOFWEEK("2025-03-20")) # dow = 3

# Now, start_of_week is set to Thursday,
configs.start_of_week = DayOfWeek.Thursday
configs.start_week_as_zero = False
# This would return dow = 1, as March 20, 2025 is a Thursday and is the 1st day
# of the week when the start of week is considered to be Thursday.
TPCH.calculate(dow = DAYOFWEEK("2025-03-20")) # dow = 1
# If start_week_as_zero is set to True (configs.start_week_as_zero = True), 
# then DAYOFWEEK("2025-03-20") = 0
configs.start_week_as_zero = True
TPCH.calculate(dow = DAYOFWEEK("2025-03-20")) # dow = 0


# Understanding the behavior of DATETIME with `start of week`
# Set the start of the week to Monday
configs.start_of_week = DayOfWeek.MONDAY
# This would set `dt` to the start of the week for March 20, 2025 (Wednesday)
# to be March 17, 2025 (recent Monday)
TPCH.calculate(dt = DATETIME("2025-03-20", "start of week")) # dt = 2025-03-17

# start_of_week is set to Thursday,
configs.start_of_week = DayOfWeek.Thursday
# This would set `dt` to the start of the week for March 20, 2025 (Thursday)
# to be March 20, 2025 (recent Thursday)
TPCH.calculate(dt = DATETIME("2025-03-20", "start of week")) # dt = 2025-03-20
# Note: DATETIME function related to week does not depend on the 
# `start_week_as_zero` config.


# Understanding the behavior of DATEDIFF with `start of week`
# Set the start of the week to Monday
configs.start_of_week = DayOfWeek.MONDAY
# This would return 0, as the difference between March 20, 2025(Thursday) and 
# March 17, 2025(Monday) is 0 weeks when the start of week is considered to be 
# Monday. This is because both these dates fall on the same week.
TPCH.calculate(diff = DATEDIFF("2025-03-17", "2025-03-20", "week")) # diff = 0

# Set the start of the week to Thursday
configs.start_of_week = DayOfWeek.Thursday
# This would return 1, as the difference between March 20, 2025 (Thursday)
# and March 17, 2025 (Monday) is 1 week when the start of week is considered 
# to be Thursday. This is because March 17, 2025 belongs to the week starting
# from March 13, 2025 (the recent Thursday for it) and March 20, 2025 belongs 
# to the week starting from March 20, 2025 (the recent Thursday).
TPCH.calculate(diff = DATEDIFF("2025-03-17", "2025-03-20", "week")) # diff = 1
# Note: DATEDIFF function related to week does not depend on the 
# `start_week_as_zero` config.
```

<!-- TOC --><a name="session-database"></a>
### Session Database

The final core piece encapsulated by the session is the database context. A database context currently includes a connection to an actual database and an enum indicating which dialect to use.

By default, the database context contains an empty connection (which cannot be used to execute queries, but can be used to translate to SQL) and the ANSI dialect.

Just like the knowledge graph & miscellaneous configurations, the database context can also be accessed from the session and/or swapped out for another value. The APIs for creating a new database context currently take in the name of which database to use, as well as keyword arguments used by that database connector API's connection method.

Below is a list of all supported values for the database name:
- `sqlite`: uses a SQLite database. [See here](https://docs.python.org/3/library/sqlite3.html#sqlite3.connect) for details on the connection API and what keyword arguments can be passed in.

- `mysql`: uses a MySQL database. [See here](https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html) for details on the connection API and what keyword arguments can be passed in.

- `snowflake`: uses a Snowflake database. [See here](https://docs.snowflake.com/en/user-guide/python-connector.html#connecting-to-snowflake) for details on the connection API and what keyword arguments can be passed in.

- `postgres` or `postgres`: uses a Postgres database. [See here](https://www.psycopg.org/docs/) for details on the connection API and what keyword arguments can be passed in.

> Note: If you installed PyDough via pip, you can install optional connectors using pip extras:
>
> ```bash
> pip install pydough[mysql]         # Install MySQL connector
> pip install pydough[snowflake]    # Install Snowflake connector
> pip install pydough[postgres]    # Install Postgres connector
> pip install "pydough[mysql,snowflake,postgres]"  # Install all of them at once
> ```

Here’s a quick reference table showing which connector is needed for each dialect:

| Dialect    | Connector Needed                        |
|-----------|----------------------------------------|
| `sqlite`    | Already included with PyDough          |
| `mysql`     | `mysql-connector-python`               |
| `snowflake` | `snowflake-connector-python[pandas]`  |
| `postgres` | `psycopg2-binary`  |

Below are examples of how to access the context and switch it out for a newly created one, either by manually setting it or by using `session.load_database`. These examples assume that there are two different sqlite database files located at `db_files/education.db` and `db_files/shakespeare.db`.

```py
import pydough
from pydough.database_connectors import load_database_context

# Capture the original context of the active session (empty)
old_context = pydough.active_session.database

# Create a new sqlite context & set it as the database of the active session
education_context = load_database_context("sqlite", database="db_files/education.db")
pydough.active_session.database = education_context

# Same but for another database & with a different method
shakespeare_context  = pydough.active_session.load_database("sqlite", database="db_files/education.db")
```

Notice that both APIs `load_database_context` and `sesion.load_database` take in the name of the database type first and all the connection keyword arguments, and also return the context object.

It is important to ensure that the correct database context is being used for several reasons:
- It controls what SQL dialect is used when translating from PyDough to SQL.
- The context's database connection is used to execute queries once translated to SQL.

#### Examples with different supported database connectors with PyDough
- Snowflake: You can connect to a Snowflake database using `load_metadata_graph` and `connect_database` APIs. For example:
  ```py
    pydough.active_session.load_metadata_graph("../../tests/test_metadata/snowflake_sample_graphs.json", "TPCH")
    pydough.active_session.connect_database("snowflake", 
          user=snowflake_username,
          password=snowflake_password,
          account=snowflake_account,
          warehouse=snowflake_warehouse,
          database=snowflake_database,
          schema=snowflake_schema
    )
  ```
You can find a full example of using Snowflake database with PyDough in [this usage guide](./../demos/notebooks/SF_TPCH_q1.ipynb).

- MySQL: You can connect to a mysql database using `load_metadata_graph` and `connect_database` APIs. For example:
  ```py
    pydough.active_session.load_metadata_graph("../../tests/test_metadata/sample_graphs.json", "TPCH")
    pydough.active_session.connect_database("mysql", 
          user=mysql_username,
          password=mysql_password,
          database=mysql_tpch_db,
          host=mysql_host,
    )
  ```
You can find a full example of using MySQL database with PyDough in [this usage guide](./../demos/notebooks/MySQL_TPCH.ipynb).

- Postgres: You can connect to a postgres database using `load_metadata_graph` and `connect_database` APIs. For example:
  ```py
    pydough.active_session.load_metadata_graph("../../tests/test_metadata/sample_graphs.json", "TPCH")
    pydough.active_session.connect_database("postgres", 
          user=postgres_username,
          password=postgres_password,
          database=postgres_db,
          host=postgres_host,
    )
  ```
  Example with a connection object
  ```py
    pydough.active_session.load_metadata_graph("../../tests/test_metadata/sample_graphs.json", "TPCH")
    postgres_conn: psycopg2.extensions.connection = psycopg2.connect(
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port,
    )
    pydough.active_session.connect_database("postgres", connection=postgres_conn)
  ```
You can find a full example of using Postgres database with PyDough in [this usage guide](./../demos/notebooks/PG_TPCH.ipynb).

<!-- TOC --><a name="evaluation-apis"></a>
## Evaluation APIs

This section describes various APIs you can use to execute PyDough code. 

<!-- TOC --><a name="pydoughto_sql"></a>
### `pydough.to_sql`

The `to_sql` API takes in PyDough code and transforms it into SQL query text without executing it on a database. The first argument it takes in is the PyDough node for the collection being converted to SQL. It can optionally take in the following keyword arguments:

- `columns`: which columns to include in the answer if what names to call them by (if omitted, uses the names/ordering from the last `CALCULATE` clause). This can either be a non-empty list of column name strings, or a non-empty dictionary where the values are column name strings, and the keys are the strings of the aliases they should be named as.
- `metadata`: the PyDough knowledge graph to use for the conversion (if omitted, `pydough.active_session.metadata` is used instead).
- `config`: the PyDough configuration settings to use for the conversion (if omitted, `pydough.active_session.config` is used instead).
- `database`: the database context to use for the conversion (if omitted, `pydough.active_session.database` is used instead). The database context matters because it controls which SQL dialect is used for the translation.
- `session`: a PyDough session object which, if provided, is used instead of `pydough.active_session` or the `metadata` / `config` / `database` arguments. Note: this argument cannot be used alongside those arguments.

Below is an example of using `pydough.to_sql` and the output (the SQL output may be outdated if PyDough's SQL conversion process has been updated):

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
result = european_countries.CALCULATE(name, n_custs=COUNT(customers))
pydough.to_sql(result, columns=["name", "n_custs"])
```

```sql
WITH _s3 AS (
  SELECT
    c_nationkey,
    COUNT(*) AS n_rows
  FROM tpch.customer
  GROUP BY
    1
)
SELECT
  nation.n_name AS name,
  _s3.n_rows AS n_custs
FROM tpch.nation AS nation
JOIN tpch.region AS region
  ON nation.n_regionkey = region.r_regionkey AND region.r_name = 'EUROPE'
JOIN _s3 AS _s3
  ON _s3.c_nationkey = nation.n_nationkey
```

See the [demo notebooks](../demos/README.md) for more instances of how to use the `to_sql` API.

<!-- TOC --><a name="pydoughto_df"></a>
### `pydough.to_df`

The `to_df` API does all the same steps as the [`to_sql` API](#pydoughto_sql), but goes a step further and executes the query using the provided database connection, returning the result as a pandas DataFrame.  The first argument it takes in is the PyDough node for the collection being converted to SQL. It can optionally take in the following keyword arguments:


- `columns`: which columns to include in the answer if what names to call them by (if omitted, uses the names/ordering from the last `CALCULATE` clause). This can either be a non-empty list of column name strings, or a non-empty dictionary where the values are column name strings, and the keys are the strings of the aliases they should be named as.
- `metadata`: the PyDough knowledge graph to use for the conversion (if omitted, `pydough.active_session.metadata` is used instead).
- `config`: the PyDough configuration settings to use for the conversion (if omitted, `pydough.active_session.config` is used instead).
- `database`: the database context to use for the conversion (if omitted, `pydough.active_session.database` is used instead). The database context matters because it controls which SQL dialect is used for the translation.
- `session`: a PyDough session object which, if provided, is used instead of `pydough.active_session` or the `metadata` / `config` / `database` arguments. Note: this argument cannot be used alongside those arguments.
- `display_sql`: displays the sql before executing in a logger.

Below is an example of using `pydough.to_df` and the output, attached to a sqlite database containing data for the TPC-H schema:

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
result = european_countries.CALCULATE(n=COUNT(customers))
pydough.to_df(result, columns={"name": "name", "n_custs": "n"})
```

<div>
<table border="1">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>name</th>
      <th>n_custs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>FRANCE</td>
      <td>6100</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ROMANIA</td>
      <td>6100</td>
    </tr>
    <tr>
      <th>2</th>
      <td>RUSSIA</td>
      <td>6078</td>
    </tr>
    <tr>
      <th>3</th>
      <td>UNITED KINGDOM</td>
      <td>6011</td>
    </tr>
    <tr>
      <th>4</th>
      <td>GERMANY</td>
      <td>5908</td>
    </tr>
  </tbody>
</table>
</div>

See the [demo notebooks](../demos/notebooks/1_introduction.ipynb) for more instances of how to use the `to_df` API.

<!-- TOC --><a name="transformation-apis"></a>
## Transformation APIs

This section describes various APIs you can use to transform PyDough source code into a result that can be used as input for other evaluation or exploration APIs.

<!-- TOC --><a name="pydoughfrom_string"></a>
### `pydough.from_string`

The `from_string` API parses a PyDough source code string and transforms it into a PyDough collection. You can then perform operations like `explain()`, `to_sql()`, or `to_df()` on the result.

#### Syntax
```python
def from_string(
    source: str,
    answer_variable: str | None = None,
    metadata: GraphMetadata | None = None,
    environment: dict[str, Any] | None = None,
) -> UnqualifiedNode:
```

The first argument `source` is the source code string. It can be a single pydough command or a multi-line pydough code with intermediate results stored in variables. It can optionally take in the following keyword arguments:

- `answer_variable`: The name of the variable that stores the final result of the PyDough code. If not provided, the API expects the final result to be in a variable named `result`. The API returns a PyDough collection holding this value. It is assumed that the PyDough code string includes a variable definition where the name of the variable is the same as `answer_variable` and the value is valid PyDough code; if not it raises an exception.
- `metadata`: The PyDough knowledge graph to use for the transformation. If omitted, `pydough.active_session.metadata` is used.
- `environment`: A dictionary representing additional environment context. This serves as the local namespace where the PyDough code will be executed.

Below are examples of using `pydough.from_string`, and examples of the SQL that could be potentially generated from calling `pydough.to_sql` on the output. All these examples use the TPC-H dataset that can be downloaded [here](https://github.com/lovasoa/TPCH-sqlite/releases) with the [graph used in the demos directory](../demos/metadata/tpch_demo_graph.json).
 
This first example is of Python code using `pydough.from_string` to generate SQL to get the count of customers in the market segment `"AUTOMOBILE"`. The result will be returned in a variable named `pydough_query` instead of the default `result`, and the market segment `"AUTOMOBILE"` is passed in an environment variable `SEG`.:
```py
import pydough

# Setup demo metadata. Make sure you have the TPC-H dataset downloaded locally.
graph = pydough.active_session.load_metadata_graph("demos/metadata/tpch_demo_graph.json", "TPCH")
pydough.active_session.connect_database("sqlite", database="tpch.db")

# Example of a single line pydough code snippet
pydough_code = "pydough_query = TPCH.CALCULATE(n=COUNT(customers.WHERE(market_segment == SEG)))"
# Transform the pydough code and get the result from pydough_query
query = pydough.from_string(pydough_code, "pydough_query", graph, {"SEG":"AUTOMOBILE"})
sql = pydough.to_sql(query)
```

The value of `sql` is the following SQL query text as a Python string:
```sql
SELECT
  COUNT(*) AS n
FROM main.customer
WHERE
  c_mktsegment = 'AUTOMOBILE'
```

This next example is of Python code to generate SQL to get the top 5 suppliers with the highest revenue. The code snippet uses variables provided in the environment context to filter by nation, ship mode and year (`TARGET_NATION`, `DESIRED_SHIP_MODE` and `REQUESTED_SHIP_YEAR`):
```py
# Example of a multi-line pydough code snippet with intermetiate results
nation_name = "JAPAN"
ship_mode = "TRUCK"
ship_year = 1996
env = {"TARGET_NATION" : nation_name, "DESIRED_SHIP_MODE" : ship_mode, "REQUESTED_SHIP_YEAR" : ship_year}

pydough_code="""
# The supply records for the supplier that were from a medium part
selected_records = supply_records.WHERE(STARTSWITH(part.name, "coral")).CALCULATE(supply_cost)

# The revenue generated by a specific lineitem
line_revenue = extended_price * (1 - discount) * (1 - tax) - quantity * supply_cost

# The lineitem purchases for each record that were ordered in REQUESTED_SHIP_YEAR and shipped via DESIRED_SHIP_MODE
lines_year = selected_records.lines.WHERE((YEAR(ship_date) == REQUESTED_SHIP_YEAR) & (ship_mode == DESIRED_SHIP_MODE)).CALCULATE(rev=line_revenue)

# For each supplier, list their name & selected revenue from REQUESTED_SHIP_YEAR
selected_suppliers = suppliers.WHERE((nation.name == TARGET_NATION) & HAS(lines_year))
supplier_info = selected_suppliers.CALCULATE(name, revenue_year=ROUND(SUM(lines_year.rev), 2))

# Pick the 5 suppliers with the highest revenue from REQUESTED_SHIP_YEAR
result = supplier_info.TOP_K(5, by=revenue_year.DESC())
"""
# Transform the pydough code and get the result from result
query = pydough.from_string(pydough_code, environment=env)
sql = pydough.to_sql(query)
```

The value of `sql` is the following SQL query text as a Python string:
```sql
WITH _s7 AS (
  SELECT
    partsupp.ps_suppkey,
    SUM(
      lineitem.l_extendedprice * (
        1 - lineitem.l_discount
      ) * (
        1 - lineitem.l_tax
      ) - lineitem.l_quantity * partsupp.ps_supplycost
    ) AS sum_rev
  FROM main.partsupp AS partsupp
  JOIN main.part AS part
    ON part.p_name LIKE 'coral%' AND part.p_partkey = partsupp.ps_partkey
  JOIN main.lineitem AS lineitem
    ON EXTRACT(YEAR FROM CAST(lineitem.l_shipdate AS DATETIME)) = 1996
    AND lineitem.l_partkey = partsupp.ps_partkey
    AND lineitem.l_shipmode = 'TRUCK'
    AND lineitem.l_suppkey = partsupp.ps_suppkey
  GROUP BY
    1
)
SELECT
  supplier.s_name AS name,
  ROUND(COALESCE(_s7.sum_rev, 0), 2) AS revenue_year
FROM main.supplier AS supplier
JOIN main.nation AS nation
  ON nation.n_name = 'JAPAN' AND nation.n_nationkey = supplier.s_nationkey
JOIN _s7 AS _s7
  ON _s7.ps_suppkey = supplier.s_suppkey
ORDER BY
  2 DESC
LIMIT 5
```

This final example is of Python code to generate an SQL query, using 'datetime.date' passed in through the environment.
```py
# For every customer, how many urgent orders have they made in year 1996 with a 
# total price over 100000, and what is the sum of the total prices of all such 
# orders they made? Sort the result by the sum from highest to lowest, and only
# include customers with at least one such order

# For this query we set 1 environment variable and 1 function, the year 1996
# and date function from datetime. Optionally, we could import datetime.date
# from inside the pydough code string
import datetime
env = {"date" : datetime.date, "YEAR" : 1996}

pydough_code="""
selected_orders=orders.WHERE((order_priority == '1-URGENT') & (total_price > 100000) & 
  (order_date >= date(YEAR, 1, 1)) & (order_date < date(YEAR + 1, 1, 1)))

result = (
  customers
  .WHERE(HAS(selected_orders))
  .CALCULATE(name, n_orders=COUNT(selected_orders), total=SUM(selected_orders.total_price))
  .ORDER_BY(total.DESC())
)
"""

# Transform the pydough code and get the result from result
query = pydough.from_string(pydough_code, environment=env)
sql = pydough.to_sql(query)
```

The value of `sql` is the following SQL query text as a Python string:
```sql
WITH _s1 AS (
  SELECT
    o_custkey,
    COUNT(*) AS n_rows,
    SUM(o_totalprice) AS sum_o_totalprice
  FROM main.orders
  WHERE
    o_orderdate < CAST('1997-01-01' AS DATE)
    AND o_orderdate >= CAST('1996-01-01' AS DATE)
    AND o_orderpriority = '1-URGENT'
    AND o_totalprice > 100000
  GROUP BY
    1
)
SELECT
  customer.c_name AS name,
  _s1.n_rows AS n_orders,
  _s1.sum_o_totalprice AS total
FROM main.customer AS customer
JOIN _s1 AS _s1
  ON _s1.o_custkey = customer.c_custkey
ORDER BY
  3 DESC
```

<!-- TOC --><a name="exploration-apis"></a>
## Exploration APIs

This section describes various APIs you can use to explore PyDough code and figure out what each component is doing without having PyDough fully evaluate it. The following APIs take an optional `config` argument which can be used to specify the PyDough configuration settings to use for the exploration.

See the [demo notebooks](../demos/notebooks/2_exploration.ipynb) for more instances of how to use the exploration APIs.

<!-- TOC --><a name="pydoughexplain_structure"></a>
### `pydough.explain_structure`

The `explain_structure` API can be called on a PyDough knowledge graph object returned from `load_metadata_graph` or `parse_json_metadata_from_file`. It displays information about a the metadata graph, including the names of each collection in the graph, the names of all scalar and sub-collection properties for each collection, and some details of the sub-collection properties.

Below is an example that displays information about the TPC-H schema, truncated to only include nations, regions and customers.

```py
import pydough
graph = pydough.parse_json_metadata_from_file("insert_path_here.json", "TPCH")
pydough.explain_structure(graph)
```

```
Structure of PyDough graph: TPCH

  customers
  ├── acctbal
  ├── address
  ├── comment
  ├── key
  ├── mktsegment
  ├── name
  ├── nation_key
  ├── phone
  ├── nation [one member of nations]
  └── orders [multiple orders]

  nations
  ├── comment
  ├── key
  ├── name
  ├── region_key
  ├── customers [multiple customers]
  ├── region [one member of regions]
  └── suppliers [multiple suppliers]

  regions
  ├── comment
  ├── key
  ├── name
  └── nations [multiple nations]

...
```

Notice how for each collection, the scalar properties are listed first without any information, followed by the sub-collections which include information about the sub-collection they connect to, the cardinality of the connection, and the reverse property.

<!-- TOC --><a name="pydoughexplain"></a>
### `pydough.explain`

The `explain` API is a more generic explanation interface that can be called on different things to display different information:

- A metadata graph object
- A specific collection within a metadata graph object (can be accessed as `graph["collection_name"]`)
- A specific property within a specific collection within a metadata graph object (can be accessed as `graph["collection_name"]["property_name"]`)
- The PyDough code for a collection that could have `to_sql` or `to_df` called on it.

The `explain` API has the following optional arguments:
* `verbose` (default False): specifies whether to include a more detailed explanation, as opposed to a more compact summary. 
* `session` (default None): if provided, specifies what configs etc. to use when explaining PyDough code objects (if not provided, uses `pydough.active_session`).

Below are examples of each of these behaviors, using a knowledge graph for the TPCH schema.

1. Calling `explain` on the graph metadata.

```py
import pydough
graph = pydough.parse_json_metadata_from_file("insert_path_here.json", "TPCH")
pydough.explain(graph, verbose=True)
```

```
PyDough graph: TPCH
Collections:
  customers
  lines
  nations
  orders
  parts
  regions
  suppliers
  supply_records
Call pydough.explain(graph[collection_name]) to learn more about any of these collections.
Call pydough.explain_structure(graph) to see how all of the collections in the graph are connected.
```

2. Calling `explain` on a collection's metadata.

```py
import pydough
graph = pydough.parse_json_metadata_from_file("insert_path_here.json", "TPCH")
pydough.explain(graph["nations"], verbose=True)
```

```
PyDough collection: nations
Table path: main.NATION
Unique properties of collection: ['key']
Scalar properties:
  comment
  key
  name
  region_key
Subcollection properties:
  customers
  region
  suppliers
Call pydough.explain(graph['nations'][property_name]) to learn more about any of these properties
```

3a. Calling `explain` on a property's metadata (scalar attribute).

```py
import pydough
graph = pydough.parse_json_metadata_from_file("insert_path_here.json", "TPCH")
pydough.explain(graph["nations"]["name"], verbose=True)
```

```
PyDough property: nations.name
Column name: main.NATION.n_name
Data type: string
```

3b. Calling `explain` on a property's metadata (sub-collection).

```py
import pydough
graph = pydough.parse_json_metadata_from_file("insert_path_here.json", "TPCH")
pydough.explain(graph["nations"]["customers"], verbose=True)
```

```
PyDough property: nations.customers
This property connects collection nations to customers.
Cardinality of connection: One -> Many
Is reversible: yes
Reverse property: customers.nation
The subcollection relationship is defined by the following join conditions:
    nations.key == customers.nation_key
```

4a. Calling `explain` on PyDough code for a collection (example 1: entire graph).

```py
%%pydough
result = TPCH
pydough.explain(result, verbose=True)
```

```
PyDough collection representing the following logic:
  TPCH

This node is a reference to the global context for the entire graph. An operation must be done onto this node (e.g. a CALCULATE or accessing a collection) before it can be executed.

The collection does not have any terms that can be included in a result if it is executed.

The collection has access to the following collections:
  customers, lines, nations, orders, parts, regions, suppliers, supply_records

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
```

4b. Calling `explain` on PyDough code for a collection (example 2: single collection).

```py
%%pydough
result = nations
pydough.explain(result, verbose=True)
```

```
PyDough collection representing the following logic:
  ──┬─ TPCH
    └─── TableCollection[nations]

This node, specifically, accesses the collection nations.
Call pydough.explain(graph['nations']) to learn more about this collection.

The following terms will be included in the result if this collection is executed:
  comment, key, name, region_key

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
```

4c. Calling `explain` on PyDough code for a collection (example 3: filtering).

```py
%%pydough
result = nations.WHERE(region.name == "EUROPE")
pydough.explain(result, verbose=True)
```

```
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Where[$1.name == 'EUROPE']
      └─┬─ AccessChild
        └─── SubCollection[region]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[region]

The main task of this node is to filter on the following conditions:
  $1.name == 'EUROPE', aka region.name == 'EUROPE'

The following terms will be included in the result if this collection is executed:
  comment, key, name, region_key

The collection has access to the following expressions:
  comment, key, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
```

4d. Calling `explain` on PyDough code for a collection (example 4: CALCULATE).

```py
%%pydough
result = nations.WHERE(region.name == "EUROPE").CALCULATE(name, n_custs=COUNT(customers))
pydough.explain(result, verbose=True)
```

```
PyDough collection representing the following logic:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    ├─┬─ Where[$1.name == 'EUROPE']
    │ └─┬─ AccessChild
    │   └─── SubCollection[region]
    └─┬─ Calculate[name=name, n_custs=COUNT($1)]
      └─┬─ AccessChild
        └─── SubCollection[customers]

This node first derives the following children before doing its main task:
  child $1:
    └─── SubCollection[customers]

The main task of this node is to calculate the following additional expressions that are added to the terms of the collection:
  n_custs <- COUNT($1), aka COUNT(customers)
  name <- name (propagated from previous collection)

The following terms will be included in the result if this collection is executed:
  n_custs, name

The collection has access to the following expressions:
  comment, key, n_custs, name, region_key

The collection has access to the following collections:
  customers, region, suppliers

Call pydough.explain_term(collection, term) to learn more about any of these
expressions or collections that the collection has access to.
```

<!-- TOC --><a name="pydoughexplain_term"></a>
### `pydough.explain_term`

The `explain` API is limited in that it can only be called on complete PyDough collections that can be passed to `to_sql` or `to_df`. For example, it would be illegal to call `explain_term` on `name` or `nations.name` because neither is a collection, unlike `nations` or `nations.customers` which are collections.

To handle cases where you need to learn about a term within a collection, you can use the `explain_term` API. The first argument to `explain_term` is PyDough code for a collection, which can have `explain` called on it, and the second is PyDough code for a term that can be evaluated within the context of that collection (e.g. a scalar term of the collection, or one of its sub-collections).

The `explain_term` API has the following optional arguments:
* `verbose` (default False): specifies whether to include a more detailed explanation, as opposed to a more compact summary. 
* `session` (default None): if provided, specifies what configs etc. to use when explaining certain terms (if not provided, uses `pydough.active_session`).

Below are examples of using `explain_term`, using a knowledge graph for the TPCH schema. For each of these examples, `european_countries` is the "context" collection, which could have `to_sql` or `to_df` called on it, and `term` is the term being explained with regards to `european_countries`.

1. Calling `explain_term` on a scalar attribute of a collection.

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
term = name
pydough.explain_term(european_countries, term, verbose=True)
```

```
Collection:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Where[$1.name == 'EUROPE']
      └─┬─ AccessChild
        └─── SubCollection[region]

The term is the following expression: name

This is column 'name' of collection 'nations'

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.nations.WHERE(region.name == 'EUROPE').CALCULATE(name)
```

2. Calling `explain_term` on a sub-collection of a collection.

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
term = customers
pydough.explain_term(european_countries, term, verbose=True)
```

```
Collection:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Where[$1.name == 'EUROPE']
      └─┬─ AccessChild
        └─── SubCollection[region]

The term is the following child of the collection:
  └─┬─ AccessChild
    └─── SubCollection[customers]

This child is plural with regards to the collection, meaning its scalar terms can only be accessed by the collection if they are aggregated.
For example, the following are valid:
  TPCH.nations.WHERE(region.name == 'EUROPE').CALCULATE(COUNT(customers.acctbal))
  TPCH.nations.WHERE(region.name == 'EUROPE').WHERE(HAS(customers))
  TPCH.nations.WHERE(region.name == 'EUROPE').ORDER_BY(COUNT(customers).DESC())

To learn more about this child, you can try calling pydough.explain on the following:
  TPCH.nations.WHERE(region.name == 'EUROPE').customers
```

3. Calling `explain_term` on a plural expression.

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
term = customers.acctbal
pydough.explain_term(european_countries, term, verbose=True)
```

```
Collection:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Where[$1.name == 'EUROPE']
      └─┬─ AccessChild
        └─── SubCollection[region]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── SubCollection[customers]

The term is the following expression: $1.acctbal

This is a reference to expression 'acctbal' of child $1

This expression is plural with regards to the collection, meaning it can be placed in a CALCULATE of a collection if it is aggregated.
For example, the following is valid:
  TPCH.nations.WHERE(region.name == 'EUROPE').CALCULATE(COUNT(customers.acctbal))
```

4. Calling `explain_term` on an aggregation function call.

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
term = AVG(customers.acctbal)
pydough.explain_term(european_countries, term, verbose=True)
```

```
Collection:
  ──┬─ TPCH
    ├─── TableCollection[nations]
    └─┬─ Where[$1.name == 'EUROPE']
      └─┬─ AccessChild
        └─── SubCollection[region]

The evaluation of this term first derives the following additional children to the collection before doing its main task:
  child $1:
    └─── SubCollection[customers]

The term is the following expression: AVG($1.acctbal)

This expression calls the function 'AVG' on the following arguments, aggregating them into a single value for each record of the collection:
  customers.acctbal

Call pydough.explain_term with this collection and any of the arguments to learn more about them.

This term is singular with regards to the collection, meaning it can be placed in a CALCULATE of a collection.
For example, the following is valid:
  TPCH.nations.WHERE(region.name == 'EUROPE').CALCULATE(AVG(customers.acctbal))
```

<!-- TOC --><a name="logging"></a>
## Logging

Logging is enabled and set to INFO level by default. We can change the log level by setting the environment variable `PYDOUGH_LOG_LEVEL` to the standard levels: DEBUG, INFO, WARNING, ERROR, CRITICAL.

A new `logger` object can be created using `get_logger`.
This function configures and returns a logger instance. It takes the following arguments:

- `name` : The logger's name, typically the module name (`__name__`).
- `default_level` : The default logging level if not set externally via environment variable `PYDOUGH_LOG_LEVEL`. Defaults to `logging.INFO`.
- `fmt` : An optional log message format compatible with Python's logging. The default format is `"%(asctime)s [%(levelname)s] %(name)s: %(message)s"`.
- `handlers` : An optional list of logging handlers to attach to the logger.

It returns a configured `logging.Logger` instance.
Here is an example of basic usage. We have not set the environment variable, hence the default level of logging is INFO.

```py
from pydough import get_logger
pyd_logger = get_logger(__name__)

logger.info("This is an info message.")
logger.error("This is an error message.")
```

We can also set the level of logging via a function argument. Note that if `PYDOUGH_LOG_LEVEL` is available, the default_level argument is overridden. 

```python
# Import the function
from pydough import get_logger

# Get logger with a custom name and level
logger = get_logger(name="custom_logger", default_level=logging.DEBUG)

# Log messages
logger.debug("This is a debug message.")
logger.warning("This is a warning message.")
```
We can also attach other handlers in addition to the default handler(`logging.StreamHandler(sys.stdout)`), by sending a list of handlers.

```python
import logging
from pydough import get_logger

# Create a file handler
file_handler = logging.FileHandler("logfile.log")

# Get logger with custom file handler
logger = get_logger(handlers=[file_handler])

# Log messages
logger.info("This message will go to the console and the file.")
```