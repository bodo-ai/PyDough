# PyDough Training Module

This module contains information that can be used to aid in training an LLM for PyDough.

## Corpus Format

A training set of PyDough question-answer pairs is located in `pydough_corpus.csv`. The data format is as follows:
- `graph`: The name of the graph being used to explore the data.
- `question`: The text for the question being asked.
- `valid`: Whether the question has a valid PyDough answer (Y or N).
- `output`:
    * If valid: the PyDough code to answer the question. The final answer should be a PyDough variable stored in a variable named `"result"`.
    * If invalid: an error message explaining why the question cannot be answered by PyDough.
- `sql_text` (optional): An equivalent SQL query that solves the same question.
- `sql_dialect`: The dialect of SQL used by the code in `sql_text` (`sqlite`, `snowflake`, etc.).
- `is_benchmark`: Whether the question is a benchmark question, and therefore should NOT be used in LLM training (Y or N).

All of the graphs used by the training set are stored one of the JSON files in the `graphs` directory.

## Data Included

The following knowledge bases and queries are included:

- TPCH: the knowledge base used to describe the TPC-H schema.
    - Specific implementations of all 21 TPC-H queries.
    - Several additional custom queries using the same schema.
- Broker: the BROKER schema from the defog.ai database.
    - 7/10 of the basic questions from defog.ai in this schema.
    - 6/16 of the advanced questions from defog.ai in this schema.
