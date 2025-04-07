WITH "_t0" AS (
  SELECT DISTINCT
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
)
SELECT
  COUNT("_t0"."customer_id") AS "transaction_count"
FROM "_t0" AS "_t0"
JOIN "main"."sbcustomer" AS "sbcustomer"
  ON "_t0"."customer_id" = "sbcustomer"."sbcustid"
  AND "sbcustomer"."sbcustjoindate" >= DATETIME('now', '-70 day')
