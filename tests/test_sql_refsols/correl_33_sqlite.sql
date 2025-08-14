WITH _t0 AS (
  SELECT
    o_orderdate
  FROM tpch.orders
), _s0 AS (
  SELECT
    MIN(o_orderdate) AS first_order_date
  FROM _t0
), _s1 AS (
  SELECT
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) AS expr_2,
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER) AS expr_3,
    COUNT(*) AS n_rows
  FROM _t0
  GROUP BY
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER),
    CAST(STRFTIME('%m', o_orderdate) AS INTEGER)
)
SELECT
  _s1.n_rows AS n
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s1.expr_2 = CAST(STRFTIME('%Y', _s0.first_order_date) AS INTEGER)
  AND _s1.expr_3 = CAST(STRFTIME('%m', _s0.first_order_date) AS INTEGER)
