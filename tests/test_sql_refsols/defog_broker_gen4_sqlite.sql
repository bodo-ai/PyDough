WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxtype" = 'sell'
    AND DATE("sbtransaction"."sbtxdatetime", 'start of day') = '2023-04-01'
  GROUP BY
    "sbtransaction"."sbtxcustid"
)
SELECT
  "sbcustomer"."sbcustid" AS "_id",
  "sbcustomer"."sbcustname" AS "name",
  COALESCE("_t1"."agg_0", 0) AS "num_tx"
FROM "main"."sbcustomer" AS "sbcustomer"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."customer_id" = "sbcustomer"."sbcustid"
ORDER BY
  "num_tx" DESC
LIMIT 1
