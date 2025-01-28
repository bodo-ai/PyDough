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
[Introduction.ipynb](notebooks/Introduction.ipynb), as this notebook is intended to explain a high-level overview of the components
of a PyDough notebook and what each step is doing. This notebook will then also explain how each
subsequent demo notebook is structured to provide insight into some aspect of PyDough.

From there, you can explore the other notebooks:
- [pydough_operations.ipynb](notebooks/pydough_operations.ipynb) demonstrates all the core operations in PyDough
- [Exploration.ipynb](notebooks/Exploration.ipynb) shows how to use several user APIs for exploring PyDough.
- [tpch.ipynb](notebooks/tpch.ipynb) provides PyDough translations for most of the TPC-H benchmark queries.
- [what_if.ipynb](notebooks/what_if.ipynb) demonstrates how to do WHAT-IF analysis with PyDough.

