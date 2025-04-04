WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "orders"."o_custkey" AS "customer_key"
  FROM "tpch"."orders" AS "orders"
  WHERE
    NOT "orders"."o_comment" LIKE '%special%requests%'
  GROUP BY
    "orders"."o_custkey"
), "_t1_2" AS (
  SELECT
    COUNT() AS "agg_0",
    COALESCE("_t1"."agg_0", 0) AS "num_non_special_orders"
  FROM "tpch"."customer" AS "customer"
  LEFT JOIN "_t1" AS "_t1"
    ON "_t1"."customer_key" = "customer"."c_custkey"
  GROUP BY
    COALESCE("_t1"."agg_0", 0)
)
SELECT
  "_t1"."num_non_special_orders" AS "C_COUNT",
  COALESCE("_t1"."agg_0", 0) AS "CUSTDIST"
FROM "_t1_2" AS "_t1"
ORDER BY
  "custdist" DESC,
  "c_count" DESC
LIMIT 10
