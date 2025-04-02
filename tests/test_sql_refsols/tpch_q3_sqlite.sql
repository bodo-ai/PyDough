WITH "_t2_2" AS (
  SELECT
    SUM("lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    )) AS "agg_0",
    "orders"."o_orderdate" AS "order_date",
    "lineitem"."l_orderkey" AS "order_key",
    "orders"."o_shippriority" AS "ship_priority"
  FROM "tpch"."orders" AS "orders"
  JOIN "tpch"."customer" AS "customer"
    ON "customer"."c_custkey" = "orders"."o_custkey"
    AND "customer"."c_mktsegment" = 'BUILDING'
  JOIN "tpch"."lineitem" AS "lineitem"
    ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
    AND "lineitem"."l_shipdate" > '1995-03-15'
  WHERE
    "orders"."o_orderdate" < '1995-03-15'
  GROUP BY
    "orders"."o_orderdate",
    "lineitem"."l_orderkey",
    "orders"."o_shippriority"
), "_t0_2" AS (
  SELECT
    "_t2"."order_key" AS "l_orderkey",
    "_t2"."order_date" AS "o_orderdate",
    "_t2"."ship_priority" AS "o_shippriority",
    COALESCE("_t2"."agg_0", 0) AS "revenue",
    COALESCE("_t2"."agg_0", 0) AS "ordering_1",
    "_t2"."order_date" AS "ordering_2",
    "_t2"."order_key" AS "ordering_3"
  FROM "_t2_2" AS "_t2"
  ORDER BY
    "ordering_1" DESC,
    "ordering_2",
    "ordering_3"
  LIMIT 10
)
SELECT
  "_t0"."l_orderkey" AS "L_ORDERKEY",
  "_t0"."revenue" AS "REVENUE",
  "_t0"."o_orderdate" AS "O_ORDERDATE",
  "_t0"."o_shippriority" AS "O_SHIPPRIORITY"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC,
  "_t0"."ordering_2",
  "_t0"."ordering_3"
