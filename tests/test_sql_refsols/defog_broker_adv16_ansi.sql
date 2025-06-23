WITH _s1 AS (
  SELECT
    (
      100.0 * (
        COALESCE(SUM(sbtxamount), 0) - COALESCE(SUM(sbtxtax + sbtxcommission), 0)
      )
    ) / COALESCE(SUM(sbtxamount), 0) AS spm,
    sbtxtickerid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH') AND sbtxtype = 'sell'
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
