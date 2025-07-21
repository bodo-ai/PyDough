"""
Logic used to generate the various sqlite databases used for PageRank tests.
"""

import sqlite3


def pagerank_configs() -> list[tuple[str, int, list[tuple[int, int]]]]:
    """
    Returns a list of configurations for generating PageRank test data.
    Each tuple contains:
    - The name of the configuration (should be in the form "PAGERANK_X").
    - The number of vertices in the graph (numbered 1 to n)
    - The list of tuples indicating edges in the graph in the form (src, dest).
    """
    configs: list[tuple[str, int, list[tuple[int, int]]]] = []
    configs.append(("PAGERANK_A", 4, [(1, 2), (2, 1), (2, 3), (3, 4), (4, 1), (4, 2)]))
    configs.append(
        ("PAGERANK_B", 5, [(1, 2), (2, 1), (2, 5), (3, 2), (4, 2), (4, 5), (5, 3)])
    )
    configs.append(
        (
            "PAGERANK_C",
            8,
            [
                (1, 2),
                (1, 6),
                (2, 1),
                (2, 5),
                (2, 6),
                (3, 2),
                (4, 2),
                (4, 5),
                (5, 3),
                (7, 8),
                (8, 7),
            ],
        )
    )
    configs.append(
        (
            "PAGERANK_D",
            16,
            [
                (1, 2),
                (1, 3),
                (1, 4),
                (1, 5),
                (2, 1),
                (2, 5),
                (3, 2),
                (4, 2),
                (4, 5),
                (4, 11),
                (5, 3),
                (5, 11),
                (5, 14),
                (5, 16),
                (6, 7),
                (7, 8),
                (8, 6),
                (8, 7),
                (9, 2),
                (9, 10),
                (11, 12),
                (12, 13),
                (12, 14),
                (13, 4),
                (13, 5),
                (15, 2),
            ],
        )
    )
    configs.append(
        ("PAGERANK_E", 5, [(i, j) for i in range(1, 6) for j in range(1, 6) if i != j])
    )
    configs.append(("PAGERANK_F", 100, []))
    configs.append(
        (
            "PAGERANK_G",
            1000,
            [
                (j + 1, i + 1)
                for i in range(1000)
                for j in range(i + 1, 1000)
                if str(i) in str(j)
            ],
        )
    )
    configs.append(
        (
            "PAGERANK_H",
            50,
            [
                (i, j)
                for i in range(1, 51)
                for j in range(1, 51)
                if i != j and (i < j or i % j == 0)
            ],
        )
    )
    return configs


def gen_pagerank_records(
    connection: sqlite3.Connection, nodes: int, edges: list[tuple[int, int]]
) -> None:
    """
    Fills a sqlite database with PageRank test data based on the provided
    configuration.

    Args:
        `connection`: The sqlite3 connection to the database.
        `nodes`: The number of nodes in the graph.
        `edges`: A list of tuples representing the edges in the graph.
    """
    cursor: sqlite3.Cursor = connection.cursor()

    # For every node, insert an entry into the SITES table.
    for site in range(nodes):
        cursor.execute(
            "INSERT INTO SITES VALUES (?, ?)",
            (site + 1, f"SITE {hex(site)[2:]:0>4}"),
        )

    # For every edge, insert an entry into the LINKS table. Keep track of
    # the nodes that have no outgoing links.
    no_outgoing: set[int] = set(range(1, nodes + 1))
    for src, dst in edges:
        no_outgoing.discard(src)
        cursor.execute(
            "INSERT INTO LINKS VALUES (?, ?)",
            (src, dst),
        )

    # If there are no outgoing links for a site, insert a NULL link for it,
    # indicating that the site links to ALL sites.
    for site in no_outgoing:
        cursor.execute(
            "INSERT INTO LINKS VALUES (?, ?)",
            (site, None),
        )

    # Insert a dummy self-link for every site.
    for site in range(1, nodes + 1):
        cursor.execute(
            "INSERT INTO LINKS VALUES (?, ?)",
            (site, site),
        )

    # Commit the changes & close the cursor
    cursor.connection.commit()
    cursor.close()
