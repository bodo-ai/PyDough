WITH "_t0" AS (
  SELECT
    COUNT("sbtransaction"."sbtxcustid") AS "agg_0",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxcustid"
)
SELECT
  SUM("_t0"."agg_0") AS "transaction_count"
FROM "_t0" AS "_t0"
JOIN "main"."sbcustomer" AS "sbcustomer"
  ON "_t0"."customer_id" = "sbcustomer"."sbcustid"
  AND "sbcustomer"."sbcustjoindate" >= DATETIME('now', '-70 day')
