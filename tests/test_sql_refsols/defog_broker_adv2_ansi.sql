WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbtransaction"."sbtxtickerid" AS "ticker_id"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -10, 'DAY'))
    AND "sbtransaction"."sbtxtype" = 'buy'
  GROUP BY
    "sbtransaction"."sbtxtickerid"
)
SELECT
  "sbticker"."sbtickersymbol" AS "symbol",
  COALESCE("_t1"."agg_0", 0) AS "tx_count"
FROM "main"."sbticker" AS "sbticker"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."ticker_id" = "sbticker"."sbtickerid"
ORDER BY
  "tx_count" DESC
LIMIT 2
