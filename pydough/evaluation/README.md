# Evaluation

This subdirectory of the PyDough directory deals with the evaluation of PyDough expressions end to end.

The evaluation module provides functionality to convert unqualified trees into actual evaluated formats, such as SQL text or the actual result of code execution.

## Available APIs

The evaluation module has the following notable APIs available for use:

- `to_sql`: Converts an unqualified tree to a SQL string. 
- `to_df`: Executes an unqualified tree and returns the results as a Pandas DataFrame.

See [evaluate_unqualified.py](evaluate_unqualified.py) for more details.
