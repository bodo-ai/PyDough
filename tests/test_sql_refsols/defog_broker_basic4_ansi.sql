WITH _t2 AS (
  SELECT
    COUNT(*) AS num_transactions,
    ANY_VALUE(sbcustomer.sbcuststate) AS sbcuststate,
    ANY_VALUE(sbticker.sbtickertype) AS sbtickertype,
    sbtransaction.sbtxcustid
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  JOIN main.sbcustomer AS sbcustomer
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbtransaction.sbtxcustid,
    sbtransaction.sbtxtickerid
), _t1 AS (
  SELECT
    SUM(num_transactions) AS num_transactions,
    ANY_VALUE(sbcuststate) AS sbcuststate,
    sbtickertype
  FROM _t2
  GROUP BY
    sbtickertype,
    sbtxcustid
)
SELECT
  sbcuststate AS state,
  sbtickertype AS ticker_type,
  SUM(num_transactions) AS num_transactions
FROM _t1
GROUP BY
  sbcuststate,
  sbtickertype
ORDER BY
  num_transactions DESC
LIMIT 5
