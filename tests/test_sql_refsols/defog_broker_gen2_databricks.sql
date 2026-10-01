SELECT
  COUNT(*) AS transaction_count
FROM defog.broker.sbtransaction AS sbtransaction
JOIN defog.broker.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -70))
