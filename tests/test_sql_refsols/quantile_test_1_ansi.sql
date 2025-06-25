SELECT
  PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY
    o_totalprice NULLS LAST) AS seventieth_order_price
FROM tpch.orders
