SELECT
  _id,
  name
FROM (
  SELECT
    sbCustId AS _id,
    sbCustName AS name
  FROM main.sbCustomer
)
ANTI JOIN (
  SELECT
    sbTxCustId AS customer_id
  FROM main.sbTransaction
)
  ON _id = customer_id
