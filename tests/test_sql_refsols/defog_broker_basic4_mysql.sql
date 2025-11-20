WITH _s0 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    sbtxtickerid AS sbTxTickerId,
    COUNT(*) AS n_rows
  FROM main.sbTransaction
  GROUP BY
    1,
    2
), _s2 AS (
  SELECT
    sbTicker.sbtickertype AS sbTickerType,
    _s0.sbTxCustId,
    SUM(_s0.n_rows) AS sum_nrows
  FROM _s0 AS _s0
  JOIN main.sbTicker AS sbTicker
    ON _s0.sbTxTickerId = sbTicker.sbtickerid
  GROUP BY
    1,
    2
)
SELECT
  sbCustomer.sbcuststate AS state,
  _s2.sbTickerType AS ticker_type,
  SUM(_s2.sum_nrows) AS num_transactions
FROM _s2 AS _s2
JOIN main.sbCustomer AS sbCustomer
  ON _s2.sbTxCustId = sbCustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
