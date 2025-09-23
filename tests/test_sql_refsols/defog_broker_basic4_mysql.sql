SELECT
  sbCustomer.sbcuststate AS state,
  sbTicker.sbtickertype AS ticker_type,
  COUNT(*) AS num_transactions
FROM main.sbTransaction AS sbTransaction
JOIN main.sbTicker AS sbTicker
  ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
JOIN main.sbCustomer AS sbCustomer
  ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
