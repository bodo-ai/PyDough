WITH _s1 AS (
  SELECT
    sbtxtickerid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP - INTERVAL '10 DAY')
    AND sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS tx_count
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  2 DESC NULLS LAST
LIMIT 2
