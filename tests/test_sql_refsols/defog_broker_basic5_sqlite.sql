SELECT
  _id
FROM (
  SELECT
    sbCustId AS _id
  FROM main.sbCustomer
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        customer_id
      FROM (
        SELECT
          sbTxCustId AS customer_id,
          sbTxType AS transaction_type
        FROM main.sbTransaction
      )
      WHERE
        transaction_type = 'buy'
    )
    WHERE
      _id = customer_id
  )
