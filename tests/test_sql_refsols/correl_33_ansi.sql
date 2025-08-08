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
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS expr_2,
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) AS expr_3,
    COUNT(*) AS n_rows
  FROM _t0
  GROUP BY
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME))
)
SELECT
  _s1.n_rows AS n
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s1.expr_2 = EXTRACT(YEAR FROM CAST(_s0.first_order_date AS DATETIME))
  AND _s1.expr_3 = EXTRACT(MONTH FROM CAST(_s0.first_order_date AS DATETIME))
