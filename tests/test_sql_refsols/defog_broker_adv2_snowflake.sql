WITH _s1 AS (
  SELECT
    sbtxtickerid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', DATEADD(DAY, -10, CURRENT_TIMESTAMP()))
    AND sbtxtype = 'buy'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  _s1.n_rows AS tx_count
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  2 DESC NULLS LAST
LIMIT 2
