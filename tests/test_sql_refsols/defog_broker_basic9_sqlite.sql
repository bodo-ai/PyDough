SELECT
  _id,
  name
FROM (
  SELECT
    sbCustId AS _id,
    sbCustName AS name
  FROM main.sbCustomer
)
WHERE
  NOT EXISTS(
    SELECT
      1
    FROM (
      SELECT
        sbTxCustId AS customer_id
      FROM main.sbTransaction
    )
    WHERE
      _id = customer_id
  )
