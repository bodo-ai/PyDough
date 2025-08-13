WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM MAIN.SBCUSTOMER
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    sbcustid
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(SBTRANSACTION.sbtxamount), 0) AS total_amount
FROM MAIN.SBTRANSACTION AS SBTRANSACTION
LEFT JOIN _u_0 AS _u_0
  ON SBTRANSACTION.sbtxcustid = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
  AND SBTRANSACTION.sbtxdatetime < DATE_TRUNC(
    'DAY',
    DATEADD(
      DAY,
      (
        (
          DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
        ) % 7
      ) * -1,
      CURRENT_TIMESTAMP()
    )
  )
  AND SBTRANSACTION.sbtxdatetime >= DATEADD(
    WEEK,
    -1,
    DATE_TRUNC(
      'DAY',
      DATEADD(
        DAY,
        (
          (
            DAYOFWEEK(CURRENT_TIMESTAMP()) + 6
          ) % 7
        ) * -1,
        CURRENT_TIMESTAMP()
      )
    )
  )
