SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
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
    '-7 day'
  )
