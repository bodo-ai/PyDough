SELECT
  COUNT(sbtransaction.sbtxcustid) AS transaction_count
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= DATE(DATETIME('now', '-70 day'), 'start of day')
