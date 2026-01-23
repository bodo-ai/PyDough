WITH _t0 AS (
  SELECT
    CASE
      WHEN (
        CAST(0.3 * COUNT(o_totalprice) OVER () AS INTEGER) - CASE
          WHEN 0.3 * COUNT(o_totalprice) OVER () < CAST(0.3 * COUNT(o_totalprice) OVER () AS INTEGER)
          THEN 1
          ELSE 0
        END
      ) < ROW_NUMBER() OVER (ORDER BY o_totalprice DESC)
      THEN o_totalprice
      ELSE NULL
    END AS expr_1
  FROM tpch.orders
  WHERE
    CAST(STRFTIME('%Y', o_orderdate) AS INTEGER) = 1998
)
SELECT
  MAX(expr_1) AS seventieth_order_price
FROM _t0
