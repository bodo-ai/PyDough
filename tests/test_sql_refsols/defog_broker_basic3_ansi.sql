WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sbtxamount) AS agg_1,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  GROUP BY
    sbtxtickerid
)
SELECT
  sbticker.sbtickersymbol AS symbol,
  COALESCE(_s1.agg_0, 0) AS num_transactions,
  COALESCE(_s1.agg_1, 0) AS total_amount
FROM main.sbticker AS sbticker
LEFT JOIN _s1 AS _s1
  ON _s1.ticker_id = sbticker.sbtickerid
ORDER BY
  total_amount DESC
LIMIT 10
