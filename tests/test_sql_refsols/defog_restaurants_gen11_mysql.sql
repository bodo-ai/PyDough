WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    rating > 4.5
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
)
SELECT
  _s0.n_rows / NULLIF(_s1.n_rows, 0) AS ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
