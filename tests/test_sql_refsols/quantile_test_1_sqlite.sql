WITH _t0 AS (
  SELECT
    CASE
      WHEN CAST(0.30000000000000004 * COUNT(o_totalprice) OVER () AS INTEGER) < ROW_NUMBER() OVER (ORDER BY o_totalprice DESC)
      THEN o_totalprice
      ELSE NULL
    END AS expr_1
  FROM tpch.orders
)
SELECT
  MAX(expr_1) AS seventieth_order_price
FROM _t0
