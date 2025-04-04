WITH _t3_2 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER)
    AND CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER) = CAST(STRFTIME('%m', sbtransaction.sbtxdatetime) AS INTEGER)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbcustomer.sbcustid
), _t0_2 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name,
    COALESCE(_t3.agg_0, 0) AS num_transactions,
    COALESCE(_t3.agg_0, 0) AS ordering_1
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _t3_2 AS _t3
    ON _t3._id = sbcustomer.sbcustid
  ORDER BY
    ordering_1 DESC
  LIMIT 1
)
SELECT
  _t0._id AS _id,
  _t0.name AS name,
  _t0.num_transactions AS num_transactions
FROM _t0_2 AS _t0
ORDER BY
  _t0.ordering_1 DESC
