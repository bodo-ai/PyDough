WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(
      (
        (
          CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS agg_1,
    DATE(
      sbtransaction.sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickertype = 'stock'
  WHERE
    sbtransaction.sbtxdatetime < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND sbtransaction.sbtxdatetime >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-56 day'
    )
  GROUP BY
    DATE(
      sbtransaction.sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
)
SELECT
  week,
  agg_0 AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
