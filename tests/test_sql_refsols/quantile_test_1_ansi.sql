WITH _s1 AS (
  SELECT
    PERCENTILE_DISC(0.7) WITHIN GROUP (ORDER BY
      o_totalprice NULLS LAST) AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  GROUP BY
    o_custkey
)
SELECT
  _s1.agg_0 AS seventieth_order_price
FROM tpch.customer AS customer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_key = customer.c_custkey
