WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
), _u_0 AS (
  SELECT
    _table_alias_1.customer_id AS _u_1
  FROM _table_alias_1 AS _table_alias_1
  GROUP BY
    _table_alias_1.customer_id
)
SELECT
  _table_alias_0._id AS _id,
  _table_alias_0.name AS name
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _u_0 AS _u_0
  ON _table_alias_0._id = _u_0._u_1
WHERE
  _u_0._u_1 IS NULL
