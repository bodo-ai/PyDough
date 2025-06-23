WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(
      (
        (
          CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS sum_is_weekend,
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
  n_rows AS num_transactions,
  COALESCE(sum_is_weekend, 0) AS weekend_transactions
FROM _t0
