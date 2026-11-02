WITH _s0 AS (
  SELECT DISTINCT
    sbTickerExchange
  FROM main.sbTicker
), _s1 AS (
  SELECT DISTINCT
    sbCustState
  FROM main.sbCustomer
), _s11 AS (
  SELECT
    _s3.sbCustState,
    _s2.sbTickerExchange,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  CROSS JOIN _s1 AS _s3
  JOIN main.sbCustomer AS sbCustomer
    ON _s3.sbCustState = sbCustomer.sbCustState
  JOIN main.sbTransaction AS sbTransaction
    ON sbCustomer.sbCustId = sbTransaction.sbTxCustId
  JOIN main.sbTicker AS sbTicker
    ON _s2.sbTickerExchange = sbTicker.sbTickerExchange
    AND sbTicker.sbTickerId = sbTransaction.sbTxTickerId
  GROUP BY
    1,
    2
)
SELECT
  _s1.sbCustState COLLATE utf8mb4_bin AS state,
  _s0.sbTickerExchange COLLATE utf8mb4_bin AS exchange,
  COALESCE(_s11.n_rows, 0) AS n
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s11 AS _s11
  ON _s0.sbTickerExchange = _s11.sbTickerExchange
  AND _s1.sbCustState = _s11.sbCustState
ORDER BY
  1,
  2
