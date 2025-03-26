WITH _table_alias_0 AS (
  SELECT
    customer.c_custkey AS key
  FROM tpch.customer AS customer
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_0,
    orders.o_custkey AS customer_key
  FROM tpch.orders AS orders
  WHERE
    NOT orders.o_comment LIKE '%special%requests%'
  GROUP BY
    orders.o_custkey
), _t2 AS (
  SELECT
    COUNT() AS agg_0,
    COALESCE(_table_alias_1.agg_0, 0) AS num_non_special_orders
  FROM _table_alias_0 AS _table_alias_0
  LEFT JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0.key = _table_alias_1.customer_key
  GROUP BY
    COALESCE(_table_alias_1.agg_0, 0)
), _t0 AS (
  SELECT
    COALESCE(_t2.agg_0, 0) AS custdist,
    _t2.num_non_special_orders AS c_count,
    COALESCE(_t2.agg_0, 0) AS ordering_1,
    _t2.num_non_special_orders AS ordering_2
  FROM _t2 AS _t2
  ORDER BY
    ordering_1 DESC,
    ordering_2 DESC
  LIMIT 10
)
SELECT
  _t0.c_count AS C_COUNT,
  _t0.custdist AS CUSTDIST
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC,
  _t0.ordering_2 DESC
