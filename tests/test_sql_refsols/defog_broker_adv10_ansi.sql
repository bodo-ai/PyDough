WITH _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM sbcustomer.sbcustjoindate) = EXTRACT(MONTH FROM sbtransaction.sbtxdatetime)
    AND EXTRACT(YEAR FROM sbcustomer.sbcustjoindate) = EXTRACT(YEAR FROM sbtransaction.sbtxdatetime)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbcustomer.sbcustid
), _t0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name,
    COALESCE(_table_alias_3.agg_0, 0) AS num_transactions,
    COALESCE(_table_alias_3.agg_0, 0) AS ordering_1
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_3._id = sbcustomer.sbcustid
  ORDER BY
    ordering_1 DESC
  LIMIT 1
)
SELECT
  _t0._id AS _id,
  _t0.name AS name,
  _t0.num_transactions AS num_transactions
FROM _t0 AS _t0
ORDER BY
  _t0.ordering_1 DESC
