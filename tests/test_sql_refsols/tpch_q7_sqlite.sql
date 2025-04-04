WITH "_t1" AS (
  SELECT
    "nation"."n_nationkey" AS "key",
    "nation"."n_name" AS "name"
  FROM "tpch"."nation" AS "nation"
), "_t1_2" AS (
  SELECT
    SUM("lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    )) AS "agg_0",
    "_t7"."name" AS "cust_nation",
    CAST(STRFTIME('%Y', "lineitem"."l_shipdate") AS INTEGER) AS "l_year",
    "_t1"."name" AS "supp_nation"
  FROM "tpch"."lineitem" AS "lineitem"
  LEFT JOIN "tpch"."supplier" AS "supplier"
    ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
  JOIN "_t1" AS "_t1"
    ON "_t1"."key" = "supplier"."s_nationkey"
  LEFT JOIN "tpch"."orders" AS "orders"
    ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  JOIN "tpch"."customer" AS "customer"
    ON "customer"."c_custkey" = "orders"."o_custkey"
  JOIN "_t1" AS "_t7"
    ON "_t7"."key" = "customer"."c_nationkey"
  WHERE
    (
      "_t1"."name" = 'FRANCE' OR "_t1"."name" = 'GERMANY'
    )
    AND (
      "_t1"."name" = 'FRANCE' OR "_t7"."name" = 'FRANCE'
    )
    AND (
      "_t1"."name" = 'GERMANY' OR "_t7"."name" = 'GERMANY'
    )
    AND (
      "_t7"."name" = 'FRANCE' OR "_t7"."name" = 'GERMANY'
    )
    AND "lineitem"."l_shipdate" <= '1996-12-31'
    AND "lineitem"."l_shipdate" >= '1995-01-01'
  GROUP BY
    "_t7"."name",
    CAST(STRFTIME('%Y', "lineitem"."l_shipdate") AS INTEGER),
    "_t1"."name"
)
SELECT
  "_t1"."supp_nation" AS "SUPP_NATION",
  "_t1"."cust_nation" AS "CUST_NATION",
  "_t1"."l_year" AS "L_YEAR",
  COALESCE("_t1"."agg_0", 0) AS "REVENUE"
FROM "_t1_2" AS "_t1"
ORDER BY
  "supp_nation",
  "cust_nation",
  "l_year"
