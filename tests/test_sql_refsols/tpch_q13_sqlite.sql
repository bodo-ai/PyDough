WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
), _t1 AS (
  SELECT
    COUNT() AS agg_0,
    COALESCE(_s1.agg_0, 0) AS num_non_special_orders
  FROM tpch.customer AS customer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_key = customer.c_custkey
  GROUP BY
    COALESCE(_s1.agg_0, 0)
)
SELECT
  num_non_special_orders AS C_COUNT,
  COALESCE(agg_0, 0) AS CUSTDIST
FROM _t1
ORDER BY
  custdist DESC,
  c_count DESC
LIMIT 10
