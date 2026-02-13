WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
), _s0 AS (
  SELECT
    MIN(o_orderdate) AS min_o_orderdate
  FROM _t0
), _s1 AS (
  SELECT
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS month_o_orderdate,
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS year_o_orderdate,
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
  ON _s1.month_o_orderdate = CAST(STRFTIME('%m', _s0.min_o_orderdate) AS INTEGER)
  AND _s1.year_o_orderdate = CAST(STRFTIME('%Y', _s0.min_o_orderdate) AS INTEGER)
