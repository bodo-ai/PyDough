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
yellow_purchases = orders.lines.WHERE(CONTAINS(part.name, "yellow"))
customer_info = customers(name, yellow_quant=SUM(yellow_purchases.quantity))
pydough.to_df(customer_info.TOP_K(10, by=yellow_quant.DESC()))
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

TODO (include `pydough.active_session`, creating a new session, swapping out sessions)

<!-- TOC --><a name="session-configs"></a>
### Session Configs

TODO (include accessing configs, editing them, creating & swapping-out configs)

<!-- TOC --><a name="session-database"></a>
### Session Database

TODO (include accessing, loading, and swapping the database)

<!-- TOC --><a name="session-knowledge-graph"></a>
### Session Knowledge Graph

TODO (include loading, accessing, and swapping the graph)

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
