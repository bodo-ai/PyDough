WITH "_t2" AS (
  SELECT DISTINCT
    "sbcustomer"."sbcustcountry" AS "country"
  FROM "main"."sbcustomer" AS "sbcustomer"
), "_t1_2" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM("sbtransaction"."sbtxamount") AS "agg_1",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    "sbtransaction"."sbtxcustid"
), "_t3_2" AS (
  SELECT
    SUM("_t1"."agg_0") AS "agg_0",
    SUM("_t1"."agg_1") AS "agg_1",
    "sbcustomer"."sbcustcountry" AS "country"
  FROM "main"."sbcustomer" AS "sbcustomer"
  JOIN "_t1_2" AS "_t1"
    ON "_t1"."customer_id" = "sbcustomer"."sbcustid"
  GROUP BY
    "sbcustomer"."sbcustcountry"
)
SELECT
  "_t2"."country" AS "country",
  COALESCE("_t3"."agg_0", 0) AS "num_transactions",
  COALESCE("_t3"."agg_1", 0) AS "total_amount"
FROM "_t2" AS "_t2"
LEFT JOIN "_t3_2" AS "_t3"
  ON "_t2"."country" = "_t3"."country"
