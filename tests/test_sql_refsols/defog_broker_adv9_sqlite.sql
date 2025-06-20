WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(
      (
        (
          CAST(STRFTIME('%w', _s0.sbtxdatetime) AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS agg_1,
    DATE(
      _s0.sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS week
  FROM main.sbtransaction AS _s0
  JOIN main.sbticker AS _s1
    ON _s0.sbtxtickerid = _s1.sbtickerid AND _s1.sbtickertype = 'stock'
  WHERE
    _s0.sbtxdatetime < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND _s0.sbtxdatetime >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-56 day'
    )
  GROUP BY
    DATE(
      _s0.sbtxdatetime,
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME(_s0.sbtxdatetime)) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
)
SELECT
  week,
  agg_0 AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
