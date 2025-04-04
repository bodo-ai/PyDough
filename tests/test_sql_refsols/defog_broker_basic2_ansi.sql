WITH "_t0" AS (
  SELECT
    AVG("sbtransaction"."sbtxshares") AS "avg_shares",
    COUNT(DISTINCT "sbtransaction"."sbtxcustid") AS "num_customers",
    "sbtransaction"."sbtxtype" AS "transaction_type"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" <= CAST('2023-03-31' AS DATE)
    AND "sbtransaction"."sbtxdatetime" >= CAST('2023-01-01' AS DATE)
  GROUP BY
    "sbtransaction"."sbtxtype"
)
SELECT
  "_t0"."transaction_type" AS "transaction_type",
  "_t0"."num_customers" AS "num_customers",
  "_t0"."avg_shares" AS "avg_shares"
FROM "_t0" AS "_t0"
ORDER BY
  "num_customers" DESC
LIMIT 3
