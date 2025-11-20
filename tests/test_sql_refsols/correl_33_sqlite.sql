WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
), _s0 AS (
  SELECT
    MIN(o_orderdate) AS min_oorderdate
  FROM _t0
), _s1 AS (
  SELECT
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month_oorderdate,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_oorderdate,
    COUNT(*) AS n_rows
  FROM _t0
  GROUP BY
    1,
    2
)
SELECT
  _s1.n_rows AS n
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s1.month_oorderdate = CAST(STRFTIME('%m', _s0.min_oorderdate) AS INTEGER)
  AND _s1.year_oorderdate = CAST(STRFTIME('%Y', _s0.min_oorderdate) AS INTEGER)
