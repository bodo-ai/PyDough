WITH _s0 AS (
  SELECT DISTINCT
    sbtickerexchange AS sbTickerExchange
  FROM main.sbTicker
), _s9 AS (
  SELECT
    sbCustomer.sbcustid AS sbCustId,
    _s2.sbTickerExchange,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  CROSS JOIN main.sbCustomer AS sbCustomer
  JOIN main.sbTransaction AS sbTransaction
    ON sbCustomer.sbcustid = sbTransaction.sbtxcustid
  JOIN main.sbTicker AS sbTicker
    ON _s2.sbTickerExchange = sbTicker.sbtickerexchange
    AND sbTicker.sbtickerid = sbTransaction.sbtxtickerid
  GROUP BY
    1,
    2
)
SELECT
  sbCustomer.sbcuststate COLLATE utf8mb4_bin AS state,
  _s0.sbTickerExchange COLLATE utf8mb4_bin AS exchange,
  SUM(_s9.n_rows) AS n
FROM _s0 AS _s0
CROSS JOIN main.sbCustomer AS sbCustomer
JOIN _s9 AS _s9
  ON _s0.sbTickerExchange = _s9.sbTickerExchange
  AND _s9.sbCustId = sbCustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  1,
  2
