WITH "_t1_2" AS (
  SELECT
    COUNT() AS "agg_1",
    SUM("sbtransaction"."sbtxamount") AS "agg_0",
    "sbtransaction"."sbtxcustid" AS "customer_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxcustid"
)
SELECT
  "sbcustomer"."sbcustname" AS "name",
  COALESCE("_t1"."agg_1", 0) AS "num_tx",
  COALESCE("_t1"."agg_0", 0) AS "total_amount",
  RANK() OVER (ORDER BY COALESCE("_t1"."agg_0", 0) DESC) AS "cust_rank"
FROM "main"."sbcustomer" AS "sbcustomer"
JOIN "_t1_2" AS "_t1"
  ON "_t1"."customer_id" = "sbcustomer"."sbcustid"
