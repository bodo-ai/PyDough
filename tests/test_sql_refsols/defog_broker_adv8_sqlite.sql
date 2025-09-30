WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM main.sbcustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    1
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbtransaction.sbtxcustid
WHERE
  NOT _u_0._u_1 IS NULL
  AND sbtransaction.sbtxdatetime < DATE(
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
