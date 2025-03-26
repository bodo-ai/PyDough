WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustjoindate AS join_date
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    MIN(sbtransaction.sbtxdatetime) AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  _table_alias_0._id AS cust_id,
  DATEDIFF(
    CAST(_table_alias_1.agg_0 AS DATETIME),
    CAST(_table_alias_0.join_date AS DATETIME),
    SECOND
  ) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.customer_id
