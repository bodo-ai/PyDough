WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM((
      (
        DAY_OF_WEEK(_s0.sbtxdatetime) + 6
      ) % 7
    ) IN (5, 6)) AS agg_1,
    DATE_TRUNC('WEEK', CAST(_s0.sbtxdatetime AS TIMESTAMP)) AS week
  FROM main.sbtransaction AS _s0
  JOIN main.sbticker AS _s1
    ON _s0.sbtxtickerid = _s1.sbtickerid AND _s1.sbtickertype = 'stock'
  WHERE
    _s0.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND _s0.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -8, 'WEEK')
  GROUP BY
    DATE_TRUNC('WEEK', CAST(_s0.sbtxdatetime AS TIMESTAMP))
)
SELECT
  week,
  agg_0 AS num_transactions,
  COALESCE(agg_1, 0) AS weekend_transactions
FROM _t0
