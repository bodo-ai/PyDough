WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    rating > 4.0
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM main.restaurant
  WHERE
    rating < 4.0
)
SELECT
  _s0.n_rows / _s1.n_rows AS ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
