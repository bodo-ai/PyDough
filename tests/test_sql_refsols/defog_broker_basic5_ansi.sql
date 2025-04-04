WITH "_t0" AS (
  SELECT
    "sbtransaction"."sbtxcustid" AS "customer_id",
    "sbtransaction"."sbtxtype" AS "transaction_type"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxtype" = 'buy'
), "_t1" AS (
  SELECT
    "_t0"."customer_id" AS "customer_id"
  FROM "_t0" AS "_t0"
), "_t0_2" AS (
  SELECT
    "sbcustomer"."sbcustid" AS "_id"
  FROM "main"."sbcustomer" AS "sbcustomer"
)
SELECT
  "_t0"."_id" AS "_id"
FROM "_t0_2" AS "_t0"
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM "_t1" AS "_t1"
    WHERE
      "_t0"."_id" = "_t1"."customer_id"
  )
