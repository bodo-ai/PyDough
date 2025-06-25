WITH _s1 AS (
  SELECT
    MAX(
      CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) > CAST((
          1.0 - 0.7
        ) * COUNT(o_totalprice) OVER (PARTITION BY o_custkey) AS INTEGER)
        THEN o_totalprice
        ELSE NULL
      END
    ) AS agg_0,
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
