WITH _s1 AS (
  SELECT
    word,
    COUNT(*) AS n_rows
  FROM dict
  GROUP BY
    1
), _s2 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shake AS shake
  LEFT JOIN _s1 AS _s1
    ON _s1.word = shake.lineword
  WHERE
    _s1.n_rows <= 0 OR _s1.n_rows IS NULL
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM shake
)
SELECT
  ROUND((
    100 * _s2.n_rows
  ) / _s3.n_rows, 2) AS pct
FROM _s2 AS _s2
CROSS JOIN _s3 AS _s3
