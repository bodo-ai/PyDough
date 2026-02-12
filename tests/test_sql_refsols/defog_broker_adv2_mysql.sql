WITH _s1 AS (
  SELECT
    sbtxtickerid AS sbTxTickerId,
    COUNT(*) AS n_rows
  FROM broker.sbTransaction
  WHERE
    sbtxdatetime >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '10' DAY) AS DATE)
    AND sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS tx_count
FROM broker.sbTicker AS sbTicker
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  2 DESC
LIMIT 2
