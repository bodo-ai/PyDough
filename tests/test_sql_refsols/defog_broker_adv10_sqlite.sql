WITH "_t3_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbcustomer"."sbcustid" AS "_id"
  FROM "main"."sbcustomer" AS "sbcustomer"
  JOIN "main"."sbtransaction" AS "sbtransaction"
    ON "sbcustomer"."sbcustid" = "sbtransaction"."sbtxcustid"
    AND CAST(STRFTIME('%Y', "sbcustomer"."sbcustjoindate") AS INTEGER) = CAST(STRFTIME('%Y', "sbtransaction"."sbtxdatetime") AS INTEGER)
    AND CAST(STRFTIME('%m', "sbcustomer"."sbcustjoindate") AS INTEGER) = CAST(STRFTIME('%m', "sbtransaction"."sbtxdatetime") AS INTEGER)
  GROUP BY
    "sbcustomer"."sbcustid"
)
SELECT
  "sbcustomer"."sbcustid" AS "_id",
  "sbcustomer"."sbcustname" AS "name",
  COALESCE("_t3"."agg_0", 0) AS "num_transactions"
FROM "main"."sbcustomer" AS "sbcustomer"
LEFT JOIN "_t3_2" AS "_t3"
  ON "_t3"."_id" = "sbcustomer"."sbcustid"
ORDER BY
  "num_transactions" DESC
LIMIT 1
