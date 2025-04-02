WITH "_t0" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbcustomer"."sbcustcountry" AS "country"
  FROM "main"."sbcustomer" AS "sbcustomer"
  WHERE
    "sbcustomer"."sbcustjoindate" >= CAST('2023-01-01' AS DATE)
  GROUP BY
    "sbcustomer"."sbcustcountry"
)
SELECT
  "_t0"."country" AS "cust_country",
  COALESCE("_t0"."agg_0", 0) AS "TAC"
FROM "_t0" AS "_t0"
