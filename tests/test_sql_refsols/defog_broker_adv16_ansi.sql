WITH _s1 AS (
  SELECT
    SUM(sbtxamount) AS agg_0,
    SUM(sbtxtax + sbtxcommission) AS agg_1,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH') AND sbtxtype = 'sell'
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_s1.agg_0, 0) - COALESCE(_s1.agg_1, 0)
    )
  ) / COALESCE(_s1.agg_0, 0) AS SPM
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.ticker_id = sbticker.sbtickerid
WHERE
  NOT (
    (
      100.0 * (
        COALESCE(_s1.agg_0, 0) - COALESCE(_s1.agg_1, 0)
      )
    ) / COALESCE(_s1.agg_0, 0)
  ) IS NULL
ORDER BY
  symbol
