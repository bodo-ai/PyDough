WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sbtransaction.sbtxamount) AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  _table_alias_0.name AS name,
  COALESCE(_table_alias_1.agg_1, 0) AS num_tx,
  COALESCE(_table_alias_1.agg_0, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(_table_alias_1.agg_0, 0) DESC NULLS FIRST) AS cust_rank
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.customer_id
