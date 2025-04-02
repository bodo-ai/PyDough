SELECT
  "orders"."o_orderkey" AS "order_key",
  RANK() OVER (ORDER BY "orders"."o_orderpriority" NULLS LAST) AS "rank"
FROM "tpch"."orders" AS "orders"
