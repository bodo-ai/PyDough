WITH _s0 AS (
  SELECT
    sbtxcustid AS customer_id,
    COUNT() AS num_transactions,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid,
    sbtxtickerid
), _s2 AS (
  SELECT
    SUM(_s0.num_transactions) AS num_transactions,
    sbticker.sbtickertype AS ticker_type,
    _s0.customer_id
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.ticker_id = sbticker.sbtickerid
  GROUP BY
    sbticker.sbtickertype,
    _s0.customer_id
), _t0 AS (
  SELECT
    SUM(_s2.num_transactions) AS num_transactions,
    sbcustomer.sbcuststate AS state,
    _s2.ticker_type
  FROM _s2 AS _s2
  JOIN main.sbcustomer AS sbcustomer
    ON _s2.customer_id = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcuststate,
    _s2.ticker_type
)
SELECT
  state,
  ticker_type,
  num_transactions
FROM _t0
ORDER BY
  num_transactions DESC
LIMIT 5
