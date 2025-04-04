WITH "_t0" AS (
  SELECT
    "sbdailyprice"."sbdpdate" AS "date",
    "sbdailyprice"."sbdptickerid" AS "ticker_id"
  FROM "main"."sbdailyprice" AS "sbdailyprice"
  WHERE
    "sbdailyprice"."sbdpdate" >= '2023-04-01'
), "_t1" AS (
  SELECT
    "_t0"."ticker_id" AS "ticker_id"
  FROM "_t0" AS "_t0"
), "_t0_2" AS (
  SELECT
    "sbticker"."sbtickerid" AS "_id"
  FROM "main"."sbticker" AS "sbticker"
)
SELECT
  "_t0"."_id" AS "_id"
FROM "_t0_2" AS "_t0"
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM "_t1" AS "_t1"
    WHERE
      "_t0"."_id" = "_t1"."ticker_id"
  )
