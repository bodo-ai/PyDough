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
), "_t0_2" AS (
  SELECT
    COALESCE("_t1"."agg_0", 0) AS "ordering_1",
    "sbticker"."sbtickersymbol" AS "symbol",
    COALESCE("_t1"."agg_0", 0) AS "tx_count"
  FROM "main"."sbticker" AS "sbticker"
  LEFT JOIN "_t1" AS "_t1"
    ON "_t1"."ticker_id" = "sbticker"."sbtickerid"
  ORDER BY
    "ordering_1" DESC
  LIMIT 2
)
SELECT
  "_t0"."symbol" AS "symbol",
  "_t0"."tx_count" AS "tx_count"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
