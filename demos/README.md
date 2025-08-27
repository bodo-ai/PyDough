The PyDough demos are a series of notebooks to introduce PyDough. PyDough is a Python compatible
DSL designed to leverage a logical document model to simplify analytics. We believe that a mature PyDough
will represent a simpler analytics experience than writing SQL or equivalent analytics libraries because:
1. PyDough can be written incrementally via expressions that are meaningless until the final context.
2. PyDough abstracts away join logic into the metadata and understanding relationships can be simplified to parent + child relationships.
3. Unlike many ORM alternatives, PyDough is not a thin 1:1 wrapper around underlying SQL syntax and instead is focus on primitive operators that answer underlying business questions.

We believe that these advantages, as well as the flexibility of a metadata layer to hide unnecessary
complexity of one's analytics universe and ability to define semantically significant names means that
PyDough simplicity will also be advantageous for LLM analytics generation.

The `notebooks` folder provides a series of Demos that one can engage with to understand PyDough's
capabilities and ultimately the experience of using PyDough. We strongly recommend starting with
[1_introduction.ipynb](notebooks/1_introduction.ipynb), as this notebook is intended to explain a high-level overview of the components
of a PyDough notebook and what each step is doing. This notebook will then also explain how each
subsequent demo notebook is structured to provide insight into some aspect of PyDough.

> [!IMPORTANT]
> Before running any notebooks, you will need to run the script to set up the TPC-H database as a local sqlite database file. You must ensure the file is located in the root directory of PyDough and is named `tpch.db`. If using `bash`, this means running the following command from the root directory of PyDough: `bash demos/setup_tpch.sh tpch.db`

Once the introduction notebook is complete, you can explore the other notebooks:
- [2_pydough_operations.ipynb](notebooks/2_pydough_operations.ipynb) demonstrates all the core operations in PyDough
- [3_exploration.ipynb](notebooks/3_exploration.ipynb) shows how to use several user APIs for exploring PyDough.
- [4_tpch.ipynb](notebooks/4_tpch.ipynb) provides PyDough translations for most of the TPC-H benchmark queries.
- [5_what_if.ipynb](notebooks/5_what_if.ipynb) demonstrates how to do WHAT-IF analysis with PyDough.
- [MySQL_TPCH.ipynb](notebooks/MySQL_TPCH.ipynb) demonstrates how to connect a MySQL database with PyDough.
- [SF_TPCH_q1.ipynb](notebooks/SF_TPCH_q1.ipynb) demonstrates how to connect a Snowflake database with PyDough.

