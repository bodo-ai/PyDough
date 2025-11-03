WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.publication
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.author
)
SELECT
  CAST(_s0.n_rows AS REAL) / CASE WHEN _s1.n_rows > 0 THEN _s1.n_rows ELSE NULL END AS publication_to_author_ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
