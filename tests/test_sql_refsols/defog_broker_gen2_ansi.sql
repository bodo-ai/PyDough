WITH _table_alias_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
), _table_alias_1 AS (
  SELECT
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustjoindate >= DATE_ADD(CURRENT_TIMESTAMP(), -70, 'DAY')
)
SELECT
  COUNT(_table_alias_0.customer_id) AS transaction_count
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.customer_id = _table_alias_1._id
