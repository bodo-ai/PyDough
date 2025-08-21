WITH _t2 AS (
  SELECT
    COUNT(*) AS num_transactions,
    ANY_VALUE(sbCustomer.sbcuststate) AS sbCustState,
    ANY_VALUE(sbTicker.sbtickertype) AS sbTickerType,
    sbTransaction.sbtxcustid AS sbTxCustId
  FROM main.sbTransaction AS sbTransaction
  JOIN main.sbTicker AS sbTicker
    ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  JOIN main.sbCustomer AS sbCustomer
    ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
  GROUP BY
    sbTransaction.sbtxtickerid,
    4
), _t1 AS (
  SELECT
    SUM(num_transactions) AS num_transactions,
    ANY_VALUE(sbCustState) AS sbCustState,
    sbTickerType
  FROM _t2
  GROUP BY
    sbTxCustId,
    3
)
SELECT
  sbCustState AS state,
  sbTickerType AS ticker_type,
  SUM(num_transactions) AS num_transactions
FROM _t1
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
