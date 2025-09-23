SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtxamount), 0) AS total_amount
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
    '-7 day'
  )
