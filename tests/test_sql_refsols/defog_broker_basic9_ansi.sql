WITH "_t1" AS (
  SELECT
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
), "_t0" AS (
  SELECT
    "sbcustomer"."sbcustid" AS "_id",
    "sbcustomer"."sbcustname" AS "name"
  FROM "main"."sbcustomer" AS "sbcustomer"
)
SELECT
  "_t0"."_id" AS "_id",
  "_t0"."name" AS "name"
FROM "_t0" AS "_t0"
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM "_t1" AS "_t1"
    WHERE
      "_t0"."_id" = "_t1"."customer_id"
  )
