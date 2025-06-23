SELECT
  DATE(
    sbtransaction.sbtxdatetime,
    '-' || CAST((
      CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) + 6
    ) % 7 AS TEXT) || ' days',
    'start of day'
  ) AS week,
  COUNT(*) AS num_transactions,
  COALESCE(
    SUM(
      (
        (
          CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ),
    0
  ) AS weekend_transactions
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
