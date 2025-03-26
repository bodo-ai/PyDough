WITH _table_alias_0 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    sbcustomer.sbcustname AS name
  FROM main.sbcustomer AS sbcustomer
), _table_alias_1 AS (
  SELECT
    sbtransaction.sbtxcustid AS customer_id
  FROM main.sbtransaction AS sbtransaction
)
SELECT
  _table_alias_0._id AS _id,
  _table_alias_0.name AS name
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0._id = _table_alias_1.customer_id
