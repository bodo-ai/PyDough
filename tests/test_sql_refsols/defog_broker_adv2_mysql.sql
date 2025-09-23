WITH _s1 AS (
  SELECT
    sbtxtickerid AS sbTxTickerId,
    COUNT(*) AS n_rows
  FROM main.sbTransaction
  WHERE
    sbtxdatetime >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '10' DAY) AS DATE)
    AND sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbTicker.sbtickersymbol AS symbol,
  _s1.n_rows AS tx_count
FROM main.sbTicker AS sbTicker
JOIN _s1 AS _s1
  ON _s1.sbTxTickerId = sbTicker.sbtickerid
ORDER BY
  2 DESC
LIMIT 2
