WITH _t2 AS (
  SELECT
    COUNT(*) AS num_transactions,
    MAX(sbcustomer.sbcuststate) AS sbcuststate,
    MAX(sbticker.sbtickertype) AS sbtickertype,
    sbtransaction.sbtxcustid
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  JOIN main.sbcustomer AS sbcustomer
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbtransaction.sbtxtickerid,
    4
), _t1 AS (
  SELECT
    SUM(num_transactions) AS num_transactions,
    MAX(sbcuststate) AS sbcuststate,
    sbtickertype
  FROM _t2
  GROUP BY
    sbtxcustid,
    3
)
SELECT
  sbcuststate AS state,
  sbtickertype AS ticker_type,
  SUM(num_transactions) AS num_transactions
FROM _t1
GROUP BY
  1,
  2
ORDER BY
  3 DESC
LIMIT 5
