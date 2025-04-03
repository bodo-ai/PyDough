WITH "_t1_2" AS (
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
    AND "lineitem"."l_shipdate" > CAST('1995-03-15' AS DATE)
  WHERE
    "orders"."o_orderdate" < CAST('1995-03-15' AS DATE)
  GROUP BY
    "orders"."o_orderdate",
    "lineitem"."l_orderkey",
    "orders"."o_shippriority"
)
SELECT
  "_t1"."order_key" AS "L_ORDERKEY",
  COALESCE("_t1"."agg_0", 0) AS "REVENUE",
  "_t1"."order_date" AS "O_ORDERDATE",
  "_t1"."ship_priority" AS "O_SHIPPRIORITY"
FROM "_t1_2" AS "_t1"
ORDER BY
  "revenue" DESC,
  "o_orderdate",
  "l_orderkey"
LIMIT 10
