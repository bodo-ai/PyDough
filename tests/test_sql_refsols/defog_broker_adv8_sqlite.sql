WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM main.sbcustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    sbcustid
), _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtransaction.sbtxamount) AS sum_sbtxamount
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
)
SELECT
  CASE WHEN n_rows > 0 THEN n_rows ELSE NULL END AS n_transactions,
  COALESCE(sum_sbtxamount, 0) AS total_amount
FROM _t0
