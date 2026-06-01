WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM postgres.main.publication
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM postgres.main.author
)
SELECT
  CAST(_s0.n_rows AS DOUBLE) / NULLIF(_s1.n_rows, 0) AS publication_to_author_ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
