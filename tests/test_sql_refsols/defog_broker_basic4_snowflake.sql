SELECT
  sbcustomer.sbcuststate AS state,
  sbticker.sbtickertype AS ticker_type,
  COUNT(*) AS num_transactions
FROM main.sbtransaction AS sbtransaction
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
JOIN main.sbcustomer AS sbcustomer
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
