WITH "_t3" AS (
  SELECT
    SUM("lineitem"."l_quantity") AS "agg_0",
    "lineitem"."l_orderkey" AS "order_key"
  FROM "tpch"."lineitem" AS "lineitem"
  GROUP BY
    "lineitem"."l_orderkey"
), "_t0_2" AS (
  SELECT
    "customer"."c_custkey" AS "c_custkey",
    "customer"."c_name" AS "c_name",
    "orders"."o_orderdate" AS "o_orderdate",
    "orders"."o_orderkey" AS "o_orderkey",
    "orders"."o_totalprice" AS "o_totalprice",
    COALESCE("_t3"."agg_0", 0) AS "total_quantity",
    "orders"."o_totalprice" AS "ordering_1",
    "orders"."o_orderdate" AS "ordering_2"
  FROM "tpch"."orders" AS "orders"
  LEFT JOIN "tpch"."customer" AS "customer"
    ON "customer"."c_custkey" = "orders"."o_custkey"
  LEFT JOIN "_t3" AS "_t3"
    ON "_t3"."order_key" = "orders"."o_orderkey"
  WHERE
    "_t3"."agg_0" > 300 AND NOT "_t3"."agg_0" IS NULL
  ORDER BY
    "ordering_1" DESC,
    "ordering_2"
  LIMIT 10
)
SELECT
  "_t0"."c_name" AS "C_NAME",
  "_t0"."c_custkey" AS "C_CUSTKEY",
  "_t0"."o_orderkey" AS "O_ORDERKEY",
  "_t0"."o_orderdate" AS "O_ORDERDATE",
  "_t0"."o_totalprice" AS "O_TOTALPRICE",
  "_t0"."total_quantity" AS "TOTAL_QUANTITY"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC,
  "_t0"."ordering_2"
