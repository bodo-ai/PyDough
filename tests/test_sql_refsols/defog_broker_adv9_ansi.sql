WITH _s0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(sbtxdatetime) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    sbtxtickerid AS ticker_id,
    DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP)) AS week
  FROM main.sbtransaction
  WHERE
    sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
  GROUP BY
    sbtxtickerid,
    DATE_TRUNC('WEEK', CAST(sbtxdatetime AS TIMESTAMP))
), _t0 AS (
  SELECT
    SUM(_s0.agg_0) AS agg_0,
    SUM(_s0.agg_1) AS agg_1,
    _s0.week
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.ticker_id = sbticker.sbtickerid AND sbticker.sbtickertype = 'stock'
  GROUP BY
    _s0.week
)
SELECT
  week,
  agg_0 AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
