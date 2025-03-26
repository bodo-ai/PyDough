WITH _table_alias_2 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_0 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) AS join_month,
    EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME)) AS join_year,
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxdatetime AS date_time
  FROM main.sbtransaction AS sbtransaction
), _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    _table_alias_0._id AS _id
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.customer_id
    AND _table_alias_0.join_month = EXTRACT(MONTH FROM CAST(_table_alias_1.date_time AS DATETIME))
    AND _table_alias_0.join_year = EXTRACT(YEAR FROM CAST(_table_alias_1.date_time AS DATETIME))
  GROUP BY
    _table_alias_0._id
), _t0 AS (
  SELECT
    _table_alias_2._id AS _id,
    _table_alias_2.name AS name,
    COALESCE(_table_alias_3.agg_0, 0) AS num_transactions,
    COALESCE(_table_alias_3.agg_0, 0) AS ordering_1
  FROM _table_alias_2 AS _table_alias_2
  LEFT JOIN _table_alias_3 AS _table_alias_3
    ON _table_alias_2._id = _table_alias_3._id
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
