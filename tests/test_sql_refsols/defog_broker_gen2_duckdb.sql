SELECT
  COUNT(*) AS transaction_count
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC('DAY', CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - INTERVAL '70' DAY)
