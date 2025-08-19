WITH _s0 AS (
  SELECT DISTINCT
    sbtickerexchange AS sbTickerExchange
  FROM main.sbTicker
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbCustomer.sbcustid AS sbCustId,
    _s2.sbTickerExchange
  FROM _s0 AS _s2
  CROSS JOIN main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
  JOIN main.sbTicker AS sbTicker
    ON _s2.sbTickerExchange = sbTicker.sbtickerexchange
    AND sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  GROUP BY
    sbCustomer.sbcustid,
    _s2.sbTickerExchange
)
SELECT
  sbCustomer.sbcuststate AS state,
  _s0.sbTickerExchange AS exchange,
  COALESCE(SUM(_s9.n_rows), 0) AS n
FROM _s0 AS _s0
CROSS JOIN main.sbCustomer AS sbCustomer
LEFT JOIN _s9 AS _s9
  ON _s0.sbTickerExchange = _s9.sbTickerExchange
  AND _s9.sbCustId = sbCustomer.sbcustid
GROUP BY
  sbCustomer.sbcuststate,
  _s0.sbTickerExchange
ORDER BY
  sbCustomer.sbcuststate COLLATE utf8mb4_bin,
  _s0.sbTickerExchange COLLATE utf8mb4_bin
