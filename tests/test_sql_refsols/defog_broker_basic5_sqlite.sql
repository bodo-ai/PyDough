WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id
  FROM main.sbcustomer AS sbcustomer
), _t0 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id,
    sbtransaction.sbtxtype AS transaction_type
  FROM main.sbtransaction AS sbtransaction
  WHERE
    sbtransaction.sbtxtype = 'buy'
), _table_alias_1 AS (
  SELECT
    _t0.customer_id AS customer_id
  FROM _t0 AS _t0
), _u_0 AS (
  SELECT
    _table_alias_1.customer_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.customer_id
)
SELECT
  _table_alias_0._id AS _id
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0._id = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
