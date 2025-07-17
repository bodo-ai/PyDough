WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbtxtickerid AS sbTxTickerId
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= DATE(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-10' DAY))
    AND sbtxtype = 'buy'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS tx_count
FROM main.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  tx_count DESC
LIMIT 2
