WITH _t0 AS (
  SELECT
    act,
    scene,
    COUNT(*) AS n_rows
  FROM shake
  WHERE
    play = 'much ado about nothing'
  GROUP BY
    1,
    2
)
SELECT
  act,
  scene,
  n_rows AS n_lines,
  ROUND((
    100 * n_rows
  ) / SUM(n_rows) OVER (), 2) AS pct_lines
FROM _t0
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
