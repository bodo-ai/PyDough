WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbcustomer"."sbcustcountry" AS "country"
  FROM "main"."sbcustomer" AS "sbcustomer"
  GROUP BY
    "sbcustomer"."sbcustcountry"
)
SELECT
  "_t1"."country" AS "country",
  COALESCE("_t1"."agg_0", 0) AS "num_customers"
FROM "_t1" AS "_t1"
ORDER BY
  "num_customers" DESC
LIMIT 5
