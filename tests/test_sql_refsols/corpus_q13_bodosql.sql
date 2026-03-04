WITH _t1 AS (
  SELECT
    act,
    AVG(CAST(LENGTH(lineword) AS DOUBLE)) OVER (PARTITION BY act) AS avg_length,
    IFF(
      LENGTH(lineword) > AVG(CAST(LENGTH(lineword) AS DOUBLE)) OVER (PARTITION BY act),
      1,
      0
    ) AS expr_4
  FROM shake
  WHERE
    play = 'loves labours lost'
)
SELECT
  act,
  ROUND(ANY_VALUE(avg_length), 2) AS avg_char_count,
  SUM(expr_4) AS n_above_avg,
  ROUND((
    100 * SUM(expr_4)
  ) / COUNT(*), 2) AS pct_above_avg
FROM _t1
GROUP BY
  1
ORDER BY
  1 NULLS FIRST
