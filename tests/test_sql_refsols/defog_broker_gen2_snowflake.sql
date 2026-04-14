SELECT
  COUNT(*) AS transaction_count
FROM broker.sbtransaction AS sbtransaction
JOIN broker.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC(
    'DAY',
    DATEADD(DAY, -70, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
  )
