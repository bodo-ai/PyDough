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
  AND sbtransaction.sbtxdatetime < DATE_TRUNC(
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
  AND sbtransaction.sbtxdatetime >= DATEADD(
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
