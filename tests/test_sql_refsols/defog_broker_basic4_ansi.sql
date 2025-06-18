WITH _t0 AS (
  SELECT
    COUNT(*) AS num_transactions,
    sbcustomer.sbcuststate AS state,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  JOIN main.sbcustomer AS sbcustomer
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbcustomer.sbcuststate,
    sbticker.sbtickertype
)
SELECT
  state,
  ticker_type,
  num_transactions
FROM _t0
ORDER BY
  num_transactions DESC
LIMIT 5
