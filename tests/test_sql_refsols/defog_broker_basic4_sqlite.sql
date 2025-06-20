WITH _s2 AS (
  SELECT
    COUNT(*) AS num_transactions,
    sbtxcustid AS customer_id,
    sbtxtickerid AS ticker_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid,
    sbtxtickerid
), _s5 AS (
  SELECT
    SUM(_s2.num_transactions) AS num_transactions_0,
    _s2.customer_id,
    _s1.sbtickertype AS ticker_type
  FROM _s2 AS _s2
  JOIN main.sbticker AS _s1
    ON _s1.sbtickerid = _s2.ticker_id
  GROUP BY
    _s2.customer_id,
    _s1.sbtickertype
), _t0 AS (
  SELECT
    SUM(_s5.num_transactions_0) AS num_transactions,
    _s4.sbcuststate AS state,
    _s5.ticker_type
  FROM _s5 AS _s5
  JOIN main.sbcustomer AS _s4
    ON _s4.sbcustid = _s5.customer_id
  GROUP BY
    _s4.sbcuststate,
    _s5.ticker_type
)
SELECT
  state,
  ticker_type,
  num_transactions
FROM _t0
ORDER BY
  num_transactions DESC
LIMIT 5
