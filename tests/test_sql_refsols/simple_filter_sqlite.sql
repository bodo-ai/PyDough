SELECT
  "orders"."o_orderdate" AS "order_date",
  "orders"."o_orderkey" AS "o_orderkey",
  "orders"."o_totalprice" AS "o_totalprice"
FROM "tpch"."orders" AS "orders"
WHERE
  "orders"."o_totalprice" < 1000.0
