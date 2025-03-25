SELECT
  _id,
  name
FROM (
  SELECT
    sbCustId AS _id,
    sbCustName AS name
  FROM main.sbCustomer
) AS _table_alias_0
ANTI JOIN (
  SELECT
    sbTxCustId AS customer_id
  FROM main.sbTransaction
) AS _table_alias_1
  ON _id = customer_id
