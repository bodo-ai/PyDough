WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows
  FROM tpch.ORDERS
  GROUP BY
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME))
)
SELECT
  MAX(n_rows) AS best_year
FROM _t0
