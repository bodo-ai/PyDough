WITH "_t6" AS (
  SELECT
    "lineitem"."l_discount" AS "discount",
    "lineitem"."l_extendedprice" AS "extended_price",
    "lineitem"."l_shipdate" AS "ship_date",
    "lineitem"."l_suppkey" AS "supplier_key"
  FROM "tpch"."lineitem" AS "lineitem"
  WHERE
    "lineitem"."l_shipdate" < '1996-04-01'
    AND "lineitem"."l_shipdate" >= '1996-01-01'
), "_t1" AS (
  SELECT
    SUM("_t6"."extended_price" * (
      1 - "_t6"."discount"
    )) AS "agg_0",
    "_t6"."supplier_key" AS "supplier_key"
  FROM "_t6" AS "_t6"
  GROUP BY
    "_t6"."supplier_key"
), "_t2_2" AS (
  SELECT
    MAX(COALESCE("_t1"."agg_0", 0)) AS "max_revenue"
  FROM "tpch"."supplier" AS "supplier"
  LEFT JOIN "_t1" AS "_t1"
    ON "_t1"."supplier_key" = "supplier"."s_suppkey"
), "_t5_2" AS (
  SELECT
    SUM("_t9"."extended_price" * (
      1 - "_t9"."discount"
    )) AS "agg_1",
    "_t9"."supplier_key" AS "supplier_key"
  FROM "_t6" AS "_t9"
  GROUP BY
    "_t9"."supplier_key"
)
SELECT
  "supplier"."s_suppkey" AS "S_SUPPKEY",
  "supplier"."s_name" AS "S_NAME",
  "supplier"."s_address" AS "S_ADDRESS",
  "supplier"."s_phone" AS "S_PHONE",
  COALESCE("_t5"."agg_1", 0) AS "TOTAL_REVENUE"
FROM "_t2_2" AS "_t2"
CROSS JOIN "tpch"."supplier" AS "supplier"
LEFT JOIN "_t5_2" AS "_t5"
  ON "_t5"."supplier_key" = "supplier"."s_suppkey"
WHERE
  "_t2"."max_revenue" = COALESCE("_t5"."agg_1", 0)
ORDER BY
  "s_suppkey"
