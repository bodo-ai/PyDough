WITH _s1 AS (
  SELECT
    sbtxtickerid,
    COUNT(*) AS n_rows
  FROM mysql.broker.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD('DAY', -10, CURRENT_TIMESTAMP))
    AND sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS tx_count
FROM mysql.broker.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  2 DESC
LIMIT 2
