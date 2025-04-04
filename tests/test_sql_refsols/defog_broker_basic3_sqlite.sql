WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM("sbtransaction"."sbtxamount") AS "agg_1",
    "sbtransaction"."sbtxtickerid" AS "ticker_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxtickerid"
)
SELECT
  "sbticker"."sbtickersymbol" AS "symbol",
  COALESCE("_t1"."agg_0", 0) AS "num_transactions",
  COALESCE("_t1"."agg_1", 0) AS "total_amount"
FROM "main"."sbticker" AS "sbticker"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."ticker_id" = "sbticker"."sbtickerid"
ORDER BY
  "total_amount" DESC
LIMIT 10
