WITH _t0 AS (
  SELECT
    CASE
      WHEN FLOOR(0.30000000000000004 * COUNT(o_totalprice) OVER ()) < ROW_NUMBER() OVER (ORDER BY o_totalprice DESC)
      THEN o_totalprice
      ELSE NULL
    END AS expr_1
  FROM tpch.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1998
)
SELECT
  MAX(expr_1) AS seventieth_order_price
FROM _t0
