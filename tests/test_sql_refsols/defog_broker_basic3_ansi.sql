WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sbtxamount) AS agg_1,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  GROUP BY
    sbtxtickerid
)
SELECT
  _s0.sbtickersymbol AS symbol,
  COALESCE(_s3.agg_0, 0) AS num_transactions,
  COALESCE(_s3.agg_1, 0) AS total_amount
FROM main.sbticker AS _s0
LEFT JOIN _s3 AS _s3
  ON _s0.sbtickerid = _s3.ticker_id
ORDER BY
  total_amount DESC
LIMIT 10
