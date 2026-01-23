WITH _t0 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year_o_orderdate,
    COUNT(*) AS n_rows
  FROM tpch.orders
  GROUP BY
    EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)),
    1
)
SELECT
  year_o_orderdate AS year,
  MAX(n_rows) AS best_month
FROM _t0
GROUP BY
  1
