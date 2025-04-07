WITH "_t0" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM(
      (
        (
          CAST(STRFTIME('%w', "sbtransaction"."sbtxdatetime") AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS "agg_1",
    "sbtransaction"."sbtxtickerid" AS "ticker_id",
    DATE(
      "sbtransaction"."sbtxdatetime",
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME("sbtransaction"."sbtxdatetime")) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS "week"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND "sbtransaction"."sbtxdatetime" >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-56 day'
    )
  GROUP BY
    "sbtransaction"."sbtxtickerid",
    DATE(
      "sbtransaction"."sbtxdatetime",
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME("sbtransaction"."sbtxdatetime")) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
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
