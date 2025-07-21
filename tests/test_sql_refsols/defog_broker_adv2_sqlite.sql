WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbtxtickerid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-10 day'), 'start of day')
    AND sbtxtype = 'buy'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.n_rows, 0) AS tx_count
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  COALESCE(_s1.n_rows, 0) DESC
LIMIT 2
