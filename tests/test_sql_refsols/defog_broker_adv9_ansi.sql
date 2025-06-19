WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(sbtransaction.sbtxdatetime) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
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
  week,
  agg_0 AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
