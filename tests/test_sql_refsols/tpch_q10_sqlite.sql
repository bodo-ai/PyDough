WITH "_t3_2" AS (
  SELECT
    SUM("lineitem"."l_extendedprice" * (
      1 - "lineitem"."l_discount"
    )) AS "agg_0",
    "orders"."o_custkey" AS "customer_key"
  FROM "tpch"."orders" AS "orders"
  JOIN "tpch"."lineitem" AS "lineitem"
    ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
    AND "lineitem"."l_returnflag" = 'R'
  WHERE
    "orders"."o_orderdate" < '1994-01-01' AND "orders"."o_orderdate" >= '1993-10-01'
  GROUP BY
    "orders"."o_custkey"
), "_t0_2" AS (
  SELECT
    "customer"."c_acctbal" AS "c_acctbal",
    "customer"."c_address" AS "c_address",
    "customer"."c_comment" AS "c_comment",
    "customer"."c_custkey" AS "c_custkey",
    "customer"."c_name" AS "c_name",
    "customer"."c_phone" AS "c_phone",
    "nation"."n_name" AS "n_name",
    COALESCE("_t3"."agg_0", 0) AS "revenue",
    COALESCE("_t3"."agg_0", 0) AS "ordering_1",
    "customer"."c_custkey" AS "ordering_2"
  FROM "tpch"."customer" AS "customer"
  LEFT JOIN "_t3_2" AS "_t3"
    ON "_t3"."customer_key" = "customer"."c_custkey"
  LEFT JOIN "tpch"."nation" AS "nation"
    ON "customer"."c_nationkey" = "nation"."n_nationkey"
  ORDER BY
    "ordering_1" DESC,
    "ordering_2"
  LIMIT 20
)
SELECT
  "_t0"."c_custkey" AS "C_CUSTKEY",
  "_t0"."c_name" AS "C_NAME",
  "_t0"."revenue" AS "REVENUE",
  "_t0"."c_acctbal" AS "C_ACCTBAL",
  "_t0"."n_name" AS "N_NAME",
  "_t0"."c_address" AS "C_ADDRESS",
  "_t0"."c_phone" AS "C_PHONE",
  "_t0"."c_comment" AS "C_COMMENT"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC,
  "_t0"."ordering_2"
