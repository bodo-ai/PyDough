WITH _s1 AS (
  SELECT
    sbtxtickerid,
    SUM(sbtxamount) AS sum_sbtxamount,
    SUM(sbtxtax + sbtxcommission) AS sum_sbtxtax_plus_sbtxcommission
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATEADD(MONTH, -1, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_s1.sum_sbtxamount, 0) - COALESCE(_s1.sum_sbtxtax_plus_sbtxcommission, 0)
    )
  ) / COALESCE(_s1.sum_sbtxamount, 0) AS SPM
FROM main.sbticker AS sbticker
JOIN _s1 AS _s1
  ON _s1.sbtxtickerid = sbticker.sbtickerid
ORDER BY
  1 NULLS FIRST
