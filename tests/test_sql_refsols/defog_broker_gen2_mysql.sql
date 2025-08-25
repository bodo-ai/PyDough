SELECT
  COUNT(sbtransaction.sbtxcustid) AS transaction_count
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  AND sbcustomer.sbcustjoindate >= CAST(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-70' DAY) AS DATE)
