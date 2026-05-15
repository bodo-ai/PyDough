SELECT
  PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY
    o_totalprice) AS seventieth_order_price
FROM tpch.orders
WHERE
  YEAR(CAST(o_orderdate AS TIMESTAMP)) = 1998
