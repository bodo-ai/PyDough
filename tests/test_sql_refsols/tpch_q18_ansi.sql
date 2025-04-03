WITH "_t3" AS (
  SELECT
    SUM("lineitem"."l_quantity") AS "agg_0",
    "lineitem"."l_orderkey" AS "order_key"
  FROM "tpch"."lineitem" AS "lineitem"
  GROUP BY
    "lineitem"."l_orderkey"
)
SELECT
  "customer"."c_name" AS "C_NAME",
  "customer"."c_custkey" AS "C_CUSTKEY",
  "orders"."o_orderkey" AS "O_ORDERKEY",
  "orders"."o_orderdate" AS "O_ORDERDATE",
  "orders"."o_totalprice" AS "O_TOTALPRICE",
  COALESCE("_t3"."agg_0", 0) AS "TOTAL_QUANTITY"
FROM "tpch"."orders" AS "orders"
LEFT JOIN "tpch"."customer" AS "customer"
  ON "customer"."c_custkey" = "orders"."o_custkey"
LEFT JOIN "_t3" AS "_t3"
  ON "_t3"."order_key" = "orders"."o_orderkey"
WHERE
  "_t3"."agg_0" > 300 AND NOT "_t3"."agg_0" IS NULL
ORDER BY
  "o_totalprice" DESC,
  "o_orderdate"
LIMIT 10
