WITH "_t0" AS (
  SELECT
    AVG("customer"."c_acctbal") AS "global_avg_balance"
  FROM "tpch"."customer" AS "customer"
  WHERE
    "customer"."c_acctbal" > 0.0
    AND SUBSTRING("customer"."c_phone", 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
), "_t3" AS (
  SELECT
    COUNT() AS "agg_0",
    "orders"."o_custkey" AS "customer_key"
  FROM "tpch"."orders" AS "orders"
  GROUP BY
    "orders"."o_custkey"
), "_t1_2" AS (
  SELECT
    COUNT() AS "agg_1",
    SUM("customer"."c_acctbal") AS "agg_2",
    SUBSTRING("customer"."c_phone", 1, 2) AS "cntry_code"
  FROM "_t0" AS "_t0"
  JOIN "tpch"."customer" AS "customer"
    ON "_t0"."global_avg_balance" < "customer"."c_acctbal"
    AND SUBSTRING("customer"."c_phone", 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  LEFT JOIN "_t3" AS "_t3"
    ON "_t3"."customer_key" = "customer"."c_custkey"
  WHERE
    "_t3"."agg_0" = 0 OR "_t3"."agg_0" IS NULL
  GROUP BY
    SUBSTRING("customer"."c_phone", 1, 2)
)
SELECT
  "_t1"."cntry_code" AS "CNTRY_CODE",
  COALESCE("_t1"."agg_1", 0) AS "NUM_CUSTS",
  COALESCE("_t1"."agg_2", 0) AS "TOTACCTBAL"
FROM "_t1_2" AS "_t1"
ORDER BY
  "_t1"."cntry_code"
