WITH _s0 AS (
  SELECT
    COUNT(*) AS num_transactions,
    sbtxcustid,
    sbtxtickerid
  FROM main.sbtransaction
  GROUP BY
    2,
    3
), _s2 AS (
  SELECT
    SUM(_s0.num_transactions) AS num_transactions,
    sbticker.sbtickertype,
    _s0.sbtxcustid
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbtxtickerid = sbticker.sbtickerid
  GROUP BY
    2,
    3
)
SELECT
  sbcustomer.sbcuststate AS state,
  _s2.sbtickertype AS ticker_type,
  SUM(_s2.num_transactions) AS num_transactions
FROM _s2 AS _s2
JOIN main.sbcustomer AS sbcustomer
  ON _s2.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  num_transactions DESC
LIMIT 5
