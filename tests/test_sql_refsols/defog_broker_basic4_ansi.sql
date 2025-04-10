WITH _t1 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustomer.sbcuststate AS state,
    sbticker.sbtickertype AS ticker_type
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  LEFT JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  GROUP BY
    sbcustomer.sbcuststate,
    sbticker.sbtickertype
)
SELECT
  state,
  ticker_type,
  COALESCE(agg_0, 0) AS num_transactions
FROM _t1
ORDER BY
  num_transactions DESC
LIMIT 5
