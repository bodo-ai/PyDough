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

TODO (include accessing, loading, and swapping the database)

<!-- TOC --><a name="evaluation-apis"></a>
## Evaluation APIs

This sections describes various APIs you can use to have PyDough execute

<!-- TOC --><a name="pydoughto_sql"></a>
### `pydough.to_sql`

TODO

<!-- TOC --><a name="pydoughto_df"></a>
### `pydough.to_df`

TODO

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
