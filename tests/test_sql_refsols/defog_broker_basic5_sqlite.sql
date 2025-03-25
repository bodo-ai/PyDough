SELECT
  _id
FROM (
  SELECT
    sbCustId AS _id
  FROM main.sbCustomer
) AS _table_alias_0
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
      ) AS _t0
      WHERE
        transaction_type = 'buy'
    ) AS _table_alias_1
    WHERE
      _id = customer_id
  )
