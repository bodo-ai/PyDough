WITH _s0 AS (
  SELECT
    sbtxcustid,
    sbtxtickerid,
    COUNT(*) AS n_rows
  FROM defog.broker.sbtransaction
  GROUP BY
    1,
    2
), _s2 AS (
  SELECT
    sbticker.sbtickertype,
    _s0.sbtxcustid,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN defog.broker.sbticker AS sbticker
    ON _s0.sbtxtickerid = sbticker.sbtickerid
  GROUP BY
    1,
    2
)
SELECT
  sbcustomer.sbcuststate AS state,
  _s2.sbtickertype AS ticker_type,
  SUM(_s2.sum_n_rows) AS num_transactions
FROM _s2 AS _s2
JOIN defog.broker.sbcustomer AS sbcustomer
  ON _s2.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
