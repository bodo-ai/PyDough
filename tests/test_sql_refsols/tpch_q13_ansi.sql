WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
), _t2 AS (
  SELECT
    COUNT() AS agg_0,
    COALESCE(_t1.agg_0, 0) AS num_non_special_orders
  FROM tpch.customer AS customer
  LEFT JOIN _t1 AS _t1
    ON _t1.customer_key = customer.c_custkey
  GROUP BY
    COALESCE(_t1.agg_0, 0)
), _t0_2 AS (
  SELECT
    COALESCE(agg_0, 0) AS custdist,
    num_non_special_orders AS c_count,
    COALESCE(agg_0, 0) AS ordering_1,
    num_non_special_orders AS ordering_2
  FROM _t2
  ORDER BY
    ordering_1 DESC,
    ordering_2 DESC
  LIMIT 10
)
SELECT
  c_count AS C_COUNT,
  custdist AS CUSTDIST
FROM _t0_2
ORDER BY
  ordering_1 DESC,
  ordering_2 DESC
