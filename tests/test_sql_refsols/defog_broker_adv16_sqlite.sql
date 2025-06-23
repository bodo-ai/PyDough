WITH _s1 AS (
  SELECT
    CAST((
      100.0 * (
        COALESCE(SUM(sbtxamount), 0) - COALESCE(SUM(sbtxtax + sbtxcommission), 0)
      )
    ) AS REAL) / COALESCE(SUM(sbtxamount), 0) AS spm,
    sbtxtickerid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATETIME('now', '-1 month') AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  _s1.spm AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  sbticker.sbtickersymbol
