WITH "_t1_2" AS (
  SELECT
    MIN("sbtransaction"."sbtxdatetime") AS "agg_0",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxcustid"
)
SELECT
  "sbcustomer"."sbcustid" AS "cust_id",
  CAST((
    (
      (
        CAST((
          JULIANDAY(DATE("_t1"."agg_0", 'start of day')) - JULIANDAY(DATE("sbcustomer"."sbcustjoindate", 'start of day'))
        ) AS INTEGER) * 24 + CAST(STRFTIME('%H', "_t1"."agg_0") AS INTEGER) - CAST(STRFTIME('%H', "sbcustomer"."sbcustjoindate") AS INTEGER)
      ) * 60 + CAST(STRFTIME('%M', "_t1"."agg_0") AS INTEGER) - CAST(STRFTIME('%M', "sbcustomer"."sbcustjoindate") AS INTEGER)
    ) * 60 + CAST(STRFTIME('%S', "_t1"."agg_0") AS INTEGER) - CAST(STRFTIME('%S', "sbcustomer"."sbcustjoindate") AS INTEGER)
  ) AS REAL) / 86400.0 AS "DaysFromJoinToFirstTransaction"
FROM "main"."sbcustomer" AS "sbcustomer"
JOIN "_t1_2" AS "_t1"
  ON "_t1"."customer_id" = "sbcustomer"."sbcustid"
