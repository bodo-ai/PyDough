WITH "_t1" AS (
  SELECT
    SUM("sbtransaction"."sbtxamount") AS "agg_0",
    SUM("sbtransaction"."sbtxtax" + "sbtransaction"."sbtxcommission") AS "agg_1",
    "sbtransaction"."sbtxtickerid" AS "ticker_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" >= DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH')
    AND "sbtransaction"."sbtxtype" = 'sell'
  GROUP BY
    "sbtransaction"."sbtxtickerid"
)
SELECT
  "sbticker"."sbtickersymbol" AS "symbol",
  (
    100.0 * (
      COALESCE("_t1"."agg_0", 0) - COALESCE("_t1"."agg_1", 0)
    )
  ) / COALESCE("_t1"."agg_0", 0) AS "SPM"
FROM "main"."sbticker" AS "sbticker"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."ticker_id" = "sbticker"."sbtickerid"
WHERE
  NOT (
    100.0 * (
      COALESCE("_t1"."agg_0", 0) - COALESCE("_t1"."agg_1", 0)
    )
  ) / COALESCE("_t1"."agg_0", 0) IS NULL
ORDER BY
  "symbol"
