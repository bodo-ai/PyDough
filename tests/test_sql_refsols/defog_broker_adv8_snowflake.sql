WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM broker.sbcustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    1
)
SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM broker.sbtransaction AS sbtransaction
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
          DAYOFWEEK(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) + 6
        ) % 7
      ) * -1,
      CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
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
            DAYOFWEEK(CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)) + 6
          ) % 7
        ) * -1,
        CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ)
      )
    )
  )
