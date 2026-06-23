SELECT
  COUNT(*) AS transaction_count
FROM mysql.broker.sbtransaction AS sbtransaction
JOIN mongo.defog.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE_TRUNC('DAY', DATE_ADD('DAY', -70, CURRENT_TIMESTAMP))
