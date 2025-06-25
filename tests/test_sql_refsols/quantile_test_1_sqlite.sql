SELECT
  MAX(
    CASE
      WHEN CAST(0.3 * COUNT(o_totalprice) OVER () AS INTEGER) < ROW_NUMBER() OVER (ORDER BY o_totalprice DESC)
      THEN o_totalprice
      ELSE NULL
    END
  ) AS seventieth_order_price
FROM tpch.orders
