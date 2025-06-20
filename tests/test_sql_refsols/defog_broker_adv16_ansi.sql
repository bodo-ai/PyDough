WITH _t0 AS (
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
  _s0.sbtickersymbol AS symbol,
  (
    100.0 * (
      COALESCE(_t0.agg_0, 0) - COALESCE(_t0.agg_1, 0)
    )
  ) / COALESCE(_t0.agg_0, 0) AS SPM
FROM main.sbticker AS _s0
JOIN _t0 AS _t0
  ON _s0.sbtickerid = _t0.ticker_id
ORDER BY
  symbol
