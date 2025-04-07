WITH "_t0" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM((
      (
        DAY_OF_WEEK("sbtransaction"."sbtxdatetime") + 6
      ) % 7
    ) IN (5, 6)) AS "agg_1",
    "sbtransaction"."sbtxtickerid" AS "ticker_id",
    DATE_TRUNC('WEEK', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP)) AS "week"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND "sbtransaction"."sbtxdatetime" >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
  GROUP BY
    "sbtransaction"."sbtxtickerid",
    DATE_TRUNC('WEEK', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP))
), "_t0_2" AS (
  SELECT
    SUM("_t0"."agg_0") AS "agg_0",
    SUM("_t0"."agg_1") AS "agg_1",
    "_t0"."week" AS "week"
  FROM "_t0" AS "_t0"
  JOIN "main"."sbticker" AS "sbticker"
    ON "_t0"."ticker_id" = "sbticker"."sbtickerid"
    AND "sbticker"."sbtickertype" = 'stock'
  GROUP BY
    "_t0"."week"
)
SELECT
  "_t0"."week" AS "week",
  COALESCE("_t0"."agg_0", 0) AS "num_transactions",
  COALESCE("_t0"."agg_1", 0) AS "weekend_transactions"
FROM "_t0_2" AS "_t0"
