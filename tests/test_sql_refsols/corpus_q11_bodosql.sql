WITH _t0 AS (
  SELECT
    act,
    scene,
    COUNT(DISTINCT dataline) AS ndistinct_dataline
  FROM shake
  WHERE
    play = 'othello'
  GROUP BY
    1,
    2
)
SELECT
  act,
  scene,
  ndistinct_dataline AS n_lines,
  ROUND(
    (
      100 * ndistinct_dataline
    ) / SUM(ndistinct_dataline) OVER (PARTITION BY act),
    2
  ) AS pct_lines
FROM _t0
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
