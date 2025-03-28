WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(CAST(DAY_OF_WEEK(sbtransaction.sbtxdatetime) AS INT) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) AS week
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickertype = 'stock'
  WHERE
    sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
  GROUP BY
    DATE_TRUNC('WEEK', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
)
SELECT
  _t0.week AS week,
  COALESCE(_t0.agg_0, 0) AS num_transactions,
  COALESCE(_t0.agg_1, 0) AS weekend_transactions
FROM _t0 AS _t0
