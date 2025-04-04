WITH "_t1" AS (
  SELECT
    MAX("sbdailyprice"."sbdphigh") AS "agg_0",
    MIN("sbdailyprice"."sbdplow") AS "agg_1",
    "sbdailyprice"."sbdptickerid" AS "ticker_id"
  FROM "main"."sbdailyprice" AS "sbdailyprice"
  WHERE
    "sbdailyprice"."sbdpdate" <= CAST('2023-04-04' AS DATE)
    AND "sbdailyprice"."sbdpdate" >= CAST('2023-04-01' AS DATE)
  GROUP BY
    "sbdailyprice"."sbdptickerid"
)
SELECT
  "sbticker"."sbtickersymbol" AS "symbol",
  "_t1"."agg_0" - "_t1"."agg_1" AS "price_change"
FROM "main"."sbticker" AS "sbticker"
LEFT JOIN "_t1" AS "_t1"
  ON "_t1"."ticker_id" = "sbticker"."sbtickerid"
ORDER BY
  "price_change" DESC
LIMIT 3
