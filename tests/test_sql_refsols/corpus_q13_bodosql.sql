WITH _t3 AS (
  SELECT
    act,
    lineword,
    play
  FROM shake
  WHERE
    play = 'loves labours lost'
), _t1 AS (
  SELECT
    act,
    AVG(CAST(LENGTH(lineword) AS DOUBLE)) OVER (PARTITION BY act) AS avg_length
  FROM _t3
), _s0 AS (
  SELECT
    act,
    ANY_VALUE(avg_length) AS anything_avg_length,
    COUNT(*) AS n_rows
  FROM _t1
  GROUP BY
    1
), _t5 AS (
  SELECT
    act,
    lineword,
    AVG(CAST(LENGTH(lineword) AS DOUBLE)) OVER (PARTITION BY act) AS avg_length
  FROM _t3
), _s1 AS (
  SELECT
    act,
    COUNT(*) AS n_rows
  FROM _t5
  WHERE
    avg_length < LENGTH(lineword)
  GROUP BY
    1
)
SELECT
  _s0.act,
  ROUND(_s0.anything_avg_length, 2) AS avg_char_count,
  COALESCE(_s1.n_rows, 0) AS n_above_avg,
  ROUND((
    100 * COALESCE(_s1.n_rows, 0)
  ) / _s0.n_rows, 2) AS pct_above_avg
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.act = _s1.act
ORDER BY
  1 NULLS FIRST
