WITH _s0 AS (
  SELECT
    COUNT(*) AS num_transactions,
    sbtxcustid AS sbTxCustId,
    sbtxtickerid AS sbTxTickerId
  FROM main.sbTransaction
  GROUP BY
    2,
    3
), _s2 AS (
  SELECT
    SUM(_s0.num_transactions) AS num_transactions,
    sbTicker.sbtickertype AS sbTickerType,
    _s0.sbTxCustId
  FROM _s0 AS _s0
  JOIN main.sbTicker AS sbTicker
    ON _s0.sbTxTickerId = sbTicker.sbtickerid
  GROUP BY
    2,
    3
)
SELECT
  sbCustomer.sbcuststate AS state,
  _s2.sbTickerType AS ticker_type,
  SUM(_s2.num_transactions) AS num_transactions
FROM _s2 AS _s2
JOIN main.sbCustomer AS sbCustomer
  ON _s2.sbTxCustId = sbCustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
