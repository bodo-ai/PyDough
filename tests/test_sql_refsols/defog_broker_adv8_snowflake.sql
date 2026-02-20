SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM broker.sbtransaction AS sbtransaction
JOIN broker.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC(
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
