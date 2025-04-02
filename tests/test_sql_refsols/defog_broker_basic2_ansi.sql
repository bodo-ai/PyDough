WITH "_t1" AS (
  SELECT
    AVG("sbtransaction"."sbtxshares") AS "avg_shares",
    COUNT(DISTINCT "sbtransaction"."sbtxcustid") AS "num_customers",
    COUNT(DISTINCT "sbtransaction"."sbtxcustid") AS "ordering_2",
    "sbtransaction"."sbtxtype" AS "transaction_type"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" <= CAST('2023-03-31' AS DATE)
    AND "sbtransaction"."sbtxdatetime" >= CAST('2023-01-01' AS DATE)
  GROUP BY
    "sbtransaction"."sbtxtype"
), "_t0" AS (
  SELECT
    "_t1"."avg_shares" AS "avg_shares",
    "_t1"."num_customers" AS "num_customers",
    "_t1"."ordering_2" AS "ordering_2",
    "_t1"."transaction_type" AS "transaction_type"
  FROM "_t1" AS "_t1"
  ORDER BY
    "ordering_2" DESC
  LIMIT 3
)
SELECT
  "_t0"."transaction_type" AS "transaction_type",
  "_t0"."num_customers" AS "num_customers",
  "_t0"."avg_shares" AS "avg_shares"
FROM "_t0" AS "_t0"
ORDER BY
  "_t0"."ordering_2" DESC
