WITH "_t2_2" AS (
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
), "_t0_2" AS (
  SELECT
    COALESCE("_t2"."agg_0", 0) AS "num_transactions",
    COALESCE("_t2"."agg_0", 0) AS "ordering_1",
    "_t2"."state" AS "state",
    "_t2"."ticker_type" AS "ticker_type"
  FROM "_t2_2" AS "_t2"
  ORDER BY
    "ordering_1" DESC
  LIMIT 5
)
SELECT
  "_t0"."state" AS "state",
  "_t0"."ticker_type" AS "ticker_type",
  "_t0"."num_transactions" AS "num_transactions"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
