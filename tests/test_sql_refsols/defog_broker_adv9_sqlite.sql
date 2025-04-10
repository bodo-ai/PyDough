WITH _s0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM((
      (
        CAST(STRFTIME('%w', sbtxdatetime) AS INTEGER) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    sbtxtickerid AS ticker_id,
    DATE(
      sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.sbtransaction
  WHERE
    sbtxdatetime < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND sbtxdatetime >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-56 day'
    )
  GROUP BY
    sbtxtickerid,
    DATE(
      sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1,
    _s0.week
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.ticker_id = sbticker.sbtickerid AND sbticker.sbtickertype = 'stock'
  GROUP BY
    _s0.week
)
SELECT
  week,
  COALESCE(agg_0, 0) AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
