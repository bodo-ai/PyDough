WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxtype" = 'sell'
    AND CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP) < CAST('2023-04-02' AS DATE)
    AND CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP) >= CAST('2023-04-01' AS DATE)
  GROUP BY
    "sbtransaction"."sbtxcustid"
), "_t0_2" AS (
  SELECT
    "sbcustomer"."sbcustid" AS "_id",
    "sbcustomer"."sbcustname" AS "name",
    COALESCE("_t1"."agg_0", 0) AS "num_tx",
    COALESCE("_t1"."agg_0", 0) AS "ordering_1"
  FROM "main"."sbcustomer" AS "sbcustomer"
  LEFT JOIN "_t1" AS "_t1"
    ON "_t1"."customer_id" = "sbcustomer"."sbcustid"
  ORDER BY
    "ordering_1" DESC
  LIMIT 1
)
SELECT
  "_t0"."_id" AS "_id",
  "_t0"."name" AS "name",
  "_t0"."num_tx" AS "num_tx"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
