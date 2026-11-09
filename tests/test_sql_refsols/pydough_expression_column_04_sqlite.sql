WITH _s0 AS (
  SELECT
    in_error_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    1
)
SELECT
  errors.er_name AS error_name,
  _s0.n_rows AS n_incidents
FROM _s0 AS _s0
JOIN main.errors AS errors
  ON _s0.in_error_id = errors.er_id
ORDER BY
  2
LIMIT 3
