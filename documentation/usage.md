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
- [Exploration APIs](#exploration-apis)
   * [`pydough.explain_structure`](#pydoughexplain_structure)
   * [`pydough.explain`](#pydoughexplain)
   * [`pydough.explain_term`](#pydoughexplain_term)

<!-- TOC end -->

<!-- TOC --><a name="setting-up-in-jupyter-notebooks"></a>
## Setting Up in Jupyter Notebooks

Once you have uv set up, you can run the command `uv run jupyter lab` from the PyDough directory to boot up a Jupyter lab process that will have access to PyDough. Once you do so, navigate to your desired notebook file, or create a new one. In that file, you will need to do the following steps in your notebook cells in order to work with PyDough:

1. Run `import pydough` to import the PyDough module.
2. Run `%load_ext pydough.jupyter_extensions` to load the PyDough Jupyter extension.
3. Connect to the metadata graph you wish to use. The metadata should be in a JSON file that can be accessed from the notebook. If the file is located at `<path>`, and the name of the graph within the file that you wish to use is `<graphname>`, then the connection command is `pydough.active_session.load_metadata_graph(<path>, <graphname>)`.
4. (optional) Connect to the database you wish to use to evaluate your analytical questions. If your database is sqlite and is located at `<path>`, the connection command is `pydough.active_session.connect_database("sqlite", database=<path>)`

So to recap, your notebook may start with a cell like this one that you will need to run before doing anything else:

```py
import pydough
%load_ext pydough.jupyter_extensions
pydough.active_session.load_metadata_graph("knowledge_graphs.json", "MyGraph")
pydough.active_session.connect_database("sqlite", "mydata.db")
```

Once you have done all of these steps, you can run PyDough in any cell of the notebook by prepending the cell with `%%pydough`. For example:

```py
%%pydough
result = nations.WHERE(region.name == "ASIA")(name, n_cust=COUNT(customers))
pydough.to_df(result)
```

Within the contents of a PyDough cell, the following is True:
- Any undefined variable is presumed to be either a function or an access to a collection/property described within the knowledge graph. This presumption is not checked until the code is fully evaluated by calling `pydough.to_df`.
    - For instance, in the example above, the following are all undefined variables that the PyDough cell resolves: `orders`, `CONTAINS`, `part`, `customers`, `name`, `SUM`, `yellow_quant`.
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
- `database`: the database context used by PyDough to determine which version of SQL to translate into and used to execute queries in a database. By default, this is no database connection until one is attached to the session. [See here](#session-database) for more details.

<!-- TOC --><a name="session-knowledge-graph"></a>
### Session Knowledge Graph

The knowledge graph contains all of the metadata used by PyDough to identify every known collection and all of its properties, including both scalar attributes and sub-collection relationships. The metadata itself is stored in an ancillary file, which currently is only allowed to be a [JSON file in the supported format](metadata.md).

There are two ways to load a knowledge graph into a session:
1. Parse the graph with `parse_json_metadata_from_file` then set `session.graph` to the returned value.
2. Use `session.load_metadata_graph`.

Both examples are shown below. These examples assume that the JSON file is located at `hello/world/graphs.json` and that the specific graph desired is called `Food` (since the file can contain multiple graphs).

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
- `sum_default_zero` (default=True): if True, then `SUM` will always return `0` instead of `NULL` when there are no records to be summed (e.g. summing over an empty sub-collection). If False, then the behavior will depend on the database being used to execute (though this nearly always means returning `NULL`).
- `avg_default_zero` (default=False): if True, then `AVG` will always return `0` instead of `NULL` when there are no records to be summed (e.g. taking the average from an empty sub-collection). If False, then the behavior will depend on the database being used to execute (though this nearly always means returning `NULL`).

For example, consider this PyDough snippet:

```py
%%pydough
selected_customers = customers.WHERE(CONTAINS(name, '2468'))
result = Nations(
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

<!-- TOC --><a name="session-database"></a>
### Session Database

The final core piece encapsulated by the session is the database context. A database context currently includes a connection to an actual database and an enum indicating which dialect to use.

By default, the database context contains an empty connection (which cannot be used to execute queries, but can be used to translate to SQL) and the ANSI dialect.

Just like the knowledge graph & miscellaneous configurations, the database context can also be accessed from the session and/or swapped out for another value. The APIs for creating a new database context currently take in the name of which database to use, as well as keyword arguments used by that database connector API's connection method.

Below is a list of all supported values for the database name:
- `sqlite`: uses a SQLite database. [See here](https://docs.python.org/3/library/sqlite3.html#sqlite3.connect) for details on the connection API and what keyword arguments can be passed in.

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

Notice that both APIs `load_database_context` and `sesion.load_database` take in the name of the databse type first and all the connection keyword arguments, and also return the context object.

It is important to ensure that the correct database context is being used for several reasons:
- It controls what SQL dialect is used when translating from PyDough to SQL.
- The context's database connection is used to execute queries once translated to SQL.

<!-- TOC --><a name="evaluation-apis"></a>
## Evaluation APIs

This sections describes various APIs you can use to execute PyDough code. 

<!-- TOC --><a name="pydoughto_sql"></a>
### `pydough.to_sql`

The `to_sql` API takes in PyDough code and transforms it into SQL query text without executing it on a database. The first argument it takes in is the PyDough node for the collection being converted to SQL. It can optionally take in the following keyword arguments:

- `metadata`: the PyDough knowledge graph to use for the conversion (if omitted, `pydough.active_session.metadata` is used instead).
- `config`: the PyDough configuration settings to use for the conversion (if omitted, `pydough.active_session.config` is used instead).
- `database`: the database context to use for the conversion (if omitted, `pydough.active_session.database` is used instead). The database context matters because it controls which SQL dialect is used for the translation.

Below is an example of using `pydough.to_sql` and the output (the SQL output may be outdated if PyDough's SQL conversion process has been updated):

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
result = european_countries(name, n_custs=COUNT(customers))
pydough.to_sql(result)
```

```sql
SELECT name, COALESCE(agg_0, 0) AS n_custs
FROM (
    SELECT name, agg_0
    FROM (
        SELECT name, key
        FROM (
            SELECT _table_alias_0.name AS name, _table_alias_0.key AS key, _table_alias_1.name AS name_3
            FROM (
                SELECT n_name AS name, n_nationkey AS key, n_regionkey AS region_key FROM main.NATION
            ) AS _table_alias_0
            LEFT JOIN (
                SELECT r_name AS name, r_regionkey AS key
                FROM main.REGION
            ) AS _table_alias_1
            ON region_key = _table_alias_1.key
        )
        WHERE name_3 = 'EUROPE'
    )
    LEFT JOIN (
        SELECT nation_key, COUNT() AS agg_0
        FROM (
            SELECT c_nationkey AS nation_key
            FROM main.CUSTOMER
        )
        GROUP BY nation_key
    )
    ON key = nation_key
)
```

See the [demo notebooks](../demos/README.md) for more instances of how to use the `to_sql` API.

<!-- TOC --><a name="pydoughto_df"></a>
### `pydough.to_df`

The `to_df` API does all the same steps as the [`to_sql` API](#pydoughto_sql), but goes a step further and executes the query using the provided database connection, returning the result as a pandas DataFrame.  The first argument it takes in is the PyDough node for the collection being converted to SQL. It can optionally take in the following keyword arguments:

- `metadata`: the PyDough knowledge graph to use for the conversion (if omitted, `pydough.active_session.metadata` is used instead).
- `config`: the PyDough configuration settings to use for the conversion (if omitted, `pydough.active_session.config` is used instead).
- `database`: the database context to use for the conversion (if omitted, `pydough.active_session.database` is used instead). The database context matters because it controls which SQL dialect is used for the translation.

Below is an example of using `pydough.to_df` and the output, attached to a sqlite database containing data for the TPC-H schema:

```py
%%pydough
european_countries = nations.WHERE(region.name == "EUROPE")
result = european_countries(name, n_custs=COUNT(customers))
pydough.to_df(result)
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

See the [demo notebooks](../demos/README.md) for more instances of how to use the `to_df` API.

<!-- TOC --><a name="exploration-apis"></a>
## Exploration APIs

This sections describes various APIs you can use to explore PyDough code and figure out what each component is doing without having PyDough fully evaluate it.

<!-- TOC --><a name="pydoughexplain_structure"></a>
### `pydough.explain_structure`

TODO

<!-- TOC --><a name="pydoughexplain"></a>
### `pydough.explain`

TODO (on a metadata or unqualified node)

<!-- TOC --><a name="pydoughexplain_term"></a>
### `pydough.explain_term`

TODO
