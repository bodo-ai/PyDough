WITH _t2_2 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustomer.sbcuststate AS state,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  GROUP BY
    sbcustomer.sbcuststate,
    sbticker.sbtickertype
), _t0_2 AS (
  SELECT
    COALESCE(agg_0, 0) AS num_transactions,
    COALESCE(agg_0, 0) AS ordering_1,
    state,
    ticker_type
  FROM _t2_2
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
SELECT
  state,
  ticker_type,
  num_transactions
FROM _t0_2
ORDER BY
  ordering_1 DESC
