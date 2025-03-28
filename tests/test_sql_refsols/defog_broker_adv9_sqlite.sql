WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(CAST(STRFTIME('%w', sbtransaction.sbtxdatetime) AS INTEGER) IN (5, 6)) AS agg_1,
    DATE(
      DATETIME(sbtransaction.sbtxdatetime),
      '-' || CAST(CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickertype = 'stock'
  WHERE
    sbtransaction.sbtxdatetime < DATE(
      DATETIME('now'),
      '-' || CAST(CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) AS TEXT) || ' days',
      'start of day'
    )
    AND sbtransaction.sbtxdatetime >= DATE(
      DATE(
        DATETIME('now'),
        '-' || CAST(CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) AS TEXT) || ' days',
        'start of day'
      ),
      '-56 day'
    )
  GROUP BY
    DATE(
      DATETIME(sbtransaction.sbtxdatetime),
      '-' || CAST(CAST(STRFTIME('%w', DATETIME(sbtransaction.sbtxdatetime)) AS INTEGER) AS TEXT) || ' days',
      'start of day'
    )
)
SELECT
  _t0.week AS week,
  COALESCE(_t0.agg_0, 0) AS num_transactions,
  COALESCE(_t0.agg_1, 0) AS weekend_transactions
FROM _t0 AS _t0
