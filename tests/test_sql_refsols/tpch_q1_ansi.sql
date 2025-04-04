WITH "_t1" AS (
  SELECT
    AVG("lineitem"."l_discount") AS "agg_0",
    AVG("lineitem"."l_extendedprice") AS "agg_1",
    AVG("lineitem"."l_quantity") AS "agg_2",
    COUNT() AS "agg_3",
    SUM(
      "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      ) * (
        1 + "lineitem"."l_tax"
      )
    ) AS "agg_5",
    SUM("lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    )) AS "agg_6",
    SUM("lineitem"."l_extendedprice") AS "agg_4",
    SUM("lineitem"."l_quantity") AS "agg_7",
    "lineitem"."l_returnflag" AS "return_flag",
    "lineitem"."l_linestatus" AS "status"
  FROM "tpch"."lineitem" AS "lineitem"
  WHERE
    "lineitem"."l_shipdate" <= CAST('1998-12-01' AS DATE)
  GROUP BY
    "lineitem"."l_returnflag",
    "lineitem"."l_linestatus"
)
SELECT
  "_t1"."return_flag" AS "L_RETURNFLAG",
  "_t1"."status" AS "L_LINESTATUS",
  COALESCE("_t1"."agg_7", 0) AS "SUM_QTY",
  COALESCE("_t1"."agg_4", 0) AS "SUM_BASE_PRICE",
  COALESCE("_t1"."agg_6", 0) AS "SUM_DISC_PRICE",
  COALESCE("_t1"."agg_5", 0) AS "SUM_CHARGE",
  "_t1"."agg_2" AS "AVG_QTY",
  "_t1"."agg_1" AS "AVG_PRICE",
  "_t1"."agg_0" AS "AVG_DISC",
  COALESCE("_t1"."agg_3", 0) AS "COUNT_ORDER"
FROM "_t1" AS "_t1"
ORDER BY
  "l_returnflag",
  "l_linestatus"
