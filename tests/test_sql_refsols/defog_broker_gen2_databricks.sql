SELECT
  COUNT(*) AS transaction_count
FROM defog.broker.sbtransaction AS sbtransaction
JOIN defog.broker.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC('DAY', DATEADD(DAY, -70, CURRENT_TIMESTAMP()))
