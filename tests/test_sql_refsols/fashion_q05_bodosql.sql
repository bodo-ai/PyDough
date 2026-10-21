WITH _s0 AS (
  SELECT
    country,
    COUNT(*) AS n_rows
  FROM sale
  GROUP BY
    1
), _s1 AS (
  SELECT
    country,
    COUNT(*) AS n_rows
  FROM sale
  WHERE
    discounted
  GROUP BY
    1
)
SELECT
  _s0.country,
  ROUND((
    100 * COALESCE(_s1.n_rows, 0)
  ) / _s0.n_rows, 2) AS pct_discounted
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.country = _s1.country
ORDER BY
  1 NULLS FIRST
