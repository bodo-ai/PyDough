WITH "_t1_2" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbcustomer"."sbcuststate" AS "state",
    "sbticker"."sbtickertype" AS "ticker_type"
  FROM "main"."sbcustomer" AS "sbcustomer"
  JOIN "main"."sbtransaction" AS "sbtransaction"
    ON "sbcustomer"."sbcustid" = "sbtransaction"."sbtxcustid"
  JOIN "main"."sbticker" AS "sbticker"
    ON "sbticker"."sbtickerid" = "sbtransaction"."sbtxtickerid"
  GROUP BY
    "sbcustomer"."sbcuststate",
    "sbticker"."sbtickertype"
)
SELECT
  "_t1"."state" AS "state",
  "_t1"."ticker_type" AS "ticker_type",
  COALESCE("_t1"."agg_0", 0) AS "num_transactions"
FROM "_t1_2" AS "_t1"
ORDER BY
  "num_transactions" DESC
LIMIT 5
