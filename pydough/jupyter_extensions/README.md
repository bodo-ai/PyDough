# PyDough Jupyter Extensions

This submodule provides Jupyter extensions for PyDough, allowing users to run PyDough code within Jupyter notebooks using magic commands.

## Available APIs

The PyDough Jupyter extensions submodule provides the following notable APIs:

- `%%pydough`: A cell magic command that allows running PyDough code within a Jupyter cell.

## Usage

To use the PyDough Jupyter extensions, you need to load the extension in your Jupyter notebook and then use the `%%pydough` magic command to run PyDough code. For example:

### Loading the Extension

First, load the PyDough Jupyter extension in your Jupyter notebook after importing pydough:

```python
%load_ext pydough.jupyter_extensions
```

### Using the `%%pydough` Magic Command

Once the extension is loaded, you can use the `%%pydough` magic command to run PyDough code within a Jupyter cell. The `%%pydough` magic command transforms the code in the cell to prepend undefined variables with `_ROOT.` and then runs the transformed code. For example:

```python
%%pydough
result = Nations.CALCULATE(
    nation_name=name,
    region_name=region.name,
    num_customers=COUNT(customers)
)
print(pydough.to_df(result))
```

The transformed code will look like this:

```python
from pydough.unqualified import UnqualifiedRoot
_ROOT = UnqualifiedRoot(pydough.active_session.metadata)

result = _ROOT.Nations.CALCULATE(
    nation_name=_ROOT.name,
    region_name=_ROOT.region.name,
    num_customers=_ROOT.COUNT(_ROOT.customers)
)
print(pydough.to_df(result))
```

## Detailed Explanation

The PyDough Jupyter extensions submodule provides a convenient way to run PyDough code within Jupyter notebooks. By using the `%%pydough` magic command, users can write PyDough code in a Jupyter cell and have it automatically transformed and executed. This allows for seamless integration of PyDough with Jupyter notebooks, making it easier to explore and analyze data using PyDough's powerful capabilities.

The `%%pydough` magic command works by transforming the code in the cell to prepend undefined variables with `_ROOT.` and then running the transformed code. This ensures that all variables are properly qualified and can be used within the context of the active PyDough metadata graph.

By using the PyDough Jupyter extensions, users can take advantage of the interactive and exploratory nature of Jupyter notebooks while leveraging the full power of PyDough for data analysis and exploration.
