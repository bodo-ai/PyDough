WITH _s0 AS (
  SELECT
    sbtxcustid,
    sbtxtickerid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  GROUP BY
    1,
    2
), _s2 AS (
  SELECT
    sbticker.sbtickertype,
    _s0.sbtxcustid,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
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
JOIN main.sbcustomer AS sbcustomer
  ON _s2.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1,
  2
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
