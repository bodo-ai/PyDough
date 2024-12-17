"""
This file maintains the relevant code that converts an unqualified tree
into an actual "evaluated" format. This is effectively the "end-to-end"
translation because the unqualified tree is the initial representation
of the code and depending on API being used, the final evaluated output
is either SQL text or the actual result of the code execution.
"""

import pandas as pd

import pydough
from pydough.configs import PyDoughConfigs
from pydough.conversion import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.sqlglot import (
    convert_dialect_to_sqlglot,
    convert_relation_to_sql,
    execute_df,
)
from pydough.unqualified import UnqualifiedNode, qualify_node

__all__ = ["to_sql", "to_df"]


def _load_session_info(
    **kwargs,
) -> tuple[GraphMetadata, PyDoughConfigs, DatabaseContext]:
    """
    Load the session information from the active session unless it is found
    in the keyword arguments.

    Args:
        **kwargs: The keyword arguments to load the session information from.

    Returns:
        tuple[PyDoughConfigs, DatabaseContext]: The configuration and Database
            context to use for translations.
    """
    metadata: GraphMetadata
    if "metadata" in kwargs:
        metadata = kwargs.pop("metadata")
    else:
        if pydough.active_session.metadata is None:
            raise ValueError(
                "Cannot evaluate Pydough without a metadata graph. "
                "Please call `pydough.active_session.load_metadata_graph()`."
            )
        metadata = pydough.active_session.metadata
    config: PyDoughConfigs
    if "config" in kwargs:
        config = kwargs.pop("config")
    else:
        config = pydough.active_session.config
    database: DatabaseContext
    if "database" in kwargs:
        database = kwargs.pop("database")
    else:
        database = pydough.active_session.database
    assert not kwargs, f"Unexpected keyword arguments: {kwargs}"
    return metadata, config, database


def to_sql(node: UnqualifiedNode, **kwargs) -> str:
    """
    Convert the given unqualified tree to a SQL string.

    Args:
        node (UnqualifiedNode): The node to convert to SQL.
        **kwargs: Additional arguments to pass to the conversion for testing.
            From a user perspective these values should always be derived from
            the active session, but to allow a simple + extensible testing
            infrastructure in the future, any of these can be passed in using
            the name of the field in session.py.

    Returns:
        str: The SQL string corresponding to the unqualified query.
    """
    graph: GraphMetadata
    config: PyDoughConfigs
    database: DatabaseContext
    graph, config, database = _load_session_info(**kwargs)
    qualified: PyDoughQDAG = qualify_node(node, graph)
    if not isinstance(qualified, PyDoughCollectionQDAG):
        raise TypeError(
            f"Final qualified expression must be a collection, found {qualified.__class__.__name__}"
        )
    relational: RelationalRoot = convert_ast_to_relational(qualified, config)
    return convert_relation_to_sql(
        relational, convert_dialect_to_sqlglot(database.dialect)
    )


def to_df(node: UnqualifiedNode, **kwargs) -> pd.DataFrame:
    """
    Execute the given unqualified tree and return the results as a Pandas
    DataFrame.

    Args:
        node (UnqualifiedNode): The node to convert to a DataFrame.
        **kwargs: Additional arguments to pass to the conversion for testing.
            From a user perspective these values should always be derived from
            the active session, but to allow a simple + extensible testing
            infrastructure in the future, any of these can be passed in using
            the name of the field in session.py.

    Returns:
        pd.DataFrame: The DataFrame corresponding to the unqualified query.
    """
    graph: GraphMetadata
    config: PyDoughConfigs
    database: DatabaseContext
    graph, config, database = _load_session_info(**kwargs)
    qualified: PyDoughQDAG = qualify_node(node, graph)
    if not isinstance(qualified, PyDoughCollectionQDAG):
        raise TypeError(
            f"Final qualified expression must be a collection, found {qualified.__class__.__name__}"
        )
    relational: RelationalRoot = convert_ast_to_relational(qualified, config)
    return execute_df(relational, database)
