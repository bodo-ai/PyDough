WITH "_T0" AS (
  SELECT
    CASE
      WHEN FLOOR(0.3 * COUNT(o_totalprice) OVER ()) < ROW_NUMBER() OVER (ORDER BY o_totalprice DESC NULLS LAST)
      THEN o_totalprice
      ELSE NULL
    END AS EXPR_1
  FROM TPCH.ORDERS
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) = 1998
)
SELECT
  MAX(EXPR_1) AS seventieth_order_price
FROM "_T0"
