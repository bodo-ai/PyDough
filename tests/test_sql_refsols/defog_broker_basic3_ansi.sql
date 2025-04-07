WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtxamount) AS agg_1,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  GROUP BY
    sbtxtickerid
), _t0_2 AS (
  SELECT
    COALESCE(_t1.agg_0, 0) AS num_transactions,
    COALESCE(_t1.agg_1, 0) AS ordering_2,
    sbticker.sbtickersymbol AS symbol,
    COALESCE(_t1.agg_1, 0) AS total_amount
  FROM main.sbticker AS sbticker
  LEFT JOIN _t1 AS _t1
    ON _t1.ticker_id = sbticker.sbtickerid
  ORDER BY
    ordering_2 DESC
  LIMIT 10
)
SELECT
  symbol,
  num_transactions,
  total_amount
FROM _t0_2
ORDER BY
  ordering_2 DESC
