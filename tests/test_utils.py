"""
TODO: add file-level docstring
"""

__all__ = ["graph_fetcher", "noun_fetcher"]

from pydough.metadata.graphs import GraphMetadata
from typing import Dict, Set, Callable

# Type alias for a function that takes in a string and generates metadata
# for a graph based on it.
graph_fetcher = Callable[[str], GraphMetadata]

# Type alias for a function that takes in a string and generates the
# representation of all the nouns in a metadata graphs based on it.
noun_fetcher = Callable[[str], Dict[str, Set[str]]]
