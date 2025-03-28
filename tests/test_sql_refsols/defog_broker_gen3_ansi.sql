WITH _table_alias_1 AS (
  SELECT
    MIN(sbtransaction.sbtxdatetime) AS agg_0,
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  DATEDIFF(_table_alias_1.agg_0, sbcustomer.sbcustjoindate, SECOND) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_1.customer_id = sbcustomer.sbcustid
