WITH "_t0" AS (
  SELECT
    MIN("sbdailyprice"."sbdpclose") AS "agg_0",
    "sbdailyprice"."sbdptickerid" AS "ticker_id"
  FROM "main"."sbdailyprice" AS "sbdailyprice"
  WHERE
    CAST((
      JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE("sbdailyprice"."sbdpdate", 'start of day'))
    ) AS INTEGER) <= 7
  GROUP BY
    "sbdailyprice"."sbdptickerid"
)
SELECT
  MIN("_t0"."agg_0") AS "lowest_price"
FROM "_t0" AS "_t0"
JOIN "main"."sbticker" AS "sbticker"
  ON "_t0"."ticker_id" = "sbticker"."sbtickerid"
  AND "sbticker"."sbtickersymbol" = 'VTI'
