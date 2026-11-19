WITH _s2 AS (
  SELECT
    DATE_ADD('MONTHS', CAST(n AS BIGINT), CAST(CAST('1992-01-01' AS DATE) AS TIMESTAMP)) AS date_val,
    MONTH(
      DATE_ADD('MONTHS', CAST(n AS BIGINT), CAST(CAST('1992-01-01' AS DATE) AS TIMESTAMP))
    ) AS mo,
    YEAR(
      DATE_ADD('MONTHS', CAST(n AS BIGINT), CAST(CAST('1992-01-01' AS DATE) AS TIMESTAMP))
    ) AS yr
  FROM TABLE(SEQUENCE(start => 0, stop => 71, step => 1))
  ORDER BY
    1 NULLS FIRST
  LIMIT 24
), _s0 AS (
  SELECT
    DATE_ADD('MONTHS', CAST(n AS BIGINT), CAST(CAST('1992-01-01' AS DATE) AS TIMESTAMP)) AS date_val,
    YEAR(
      DATE_ADD('MONTHS', CAST(n AS BIGINT), CAST(CAST('1992-01-01' AS DATE) AS TIMESTAMP))
    ) AS yr
  FROM TABLE(SEQUENCE(start => 0, stop => 71, step => 1))
  ORDER BY
    1 NULLS FIRST
  LIMIT 24
), _s3 AS (
  SELECT
    _s0.date_val,
    _s0.yr,
    COUNT(*) AS n_rows
  FROM _s0 AS _s0
  JOIN mysql.tpch.orders AS orders
    ON _s0.date_val = DATE_TRUNC('MONTH', CAST(orders.o_orderdate AS TIMESTAMP))
  GROUP BY
    1,
    2
)
SELECT
  _s2.yr AS year,
  _s2.mo AS month_of_year,
  SUM(COALESCE(_s3.n_rows, 0)) OVER (
    PARTITION BY _s2.yr
    ORDER BY _s2.mo
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS csum,
  COALESCE(_s3.n_rows, 0) - LAG(COALESCE(_s3.n_rows, 0), 1) OVER (PARTITION BY _s2.yr ORDER BY _s2.mo) AS delta
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.date_val = _s3.date_val AND _s2.yr = _s3.yr
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
