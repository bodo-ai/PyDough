WITH _table_alias_2 AS (
  SELECT DISTINCT
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
), _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxamount AS amount,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
), _table_alias_3 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(_table_alias_1.amount) AS agg_1,
    _table_alias_0.country AS country
  FROM _table_alias_0 AS _table_alias_0
  JOIN _table_alias_1 AS _table_alias_1
    ON _table_alias_0._id = _table_alias_1.customer_id
  GROUP BY
    _table_alias_0.country
)
SELECT
  _table_alias_2.country AS country,
  COALESCE(_table_alias_3.agg_0, 0) AS num_transactions,
  COALESCE(_table_alias_3.agg_1, 0) AS total_amount
FROM _table_alias_2 AS _table_alias_2
LEFT JOIN _table_alias_3 AS _table_alias_3
  ON _table_alias_2.country = _table_alias_3.country
