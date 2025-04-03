SELECT
  MIN("sbdailyprice"."sbdpclose") AS "lowest_price"
FROM "main"."sbdailyprice" AS "sbdailyprice"
JOIN "main"."sbticker" AS "sbticker"
  ON "sbdailyprice"."sbdptickerid" = "sbticker"."sbtickerid"
  AND "sbticker"."sbtickersymbol" = 'VTI'
WHERE
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE("sbdailyprice"."sbdpdate", 'start of day'))
  ) AS INTEGER) <= 7
