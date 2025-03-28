SELECT
  COUNT(customer_id) AS transaction_count
FROM (
  SELECT
    customer_id
  FROM (
    SELECT
      sbTxCustId AS customer_id
    FROM main.sbTransaction
  )
  INNER JOIN (
    SELECT
      _id
    FROM (
      SELECT
        sbCustId AS _id,
        sbCustJoinDate AS join_date
      FROM main.sbCustomer
    )
    WHERE
      join_date >= DATE_ADD(CURRENT_TIMESTAMP(), -70, 'DAY')
  )
    ON customer_id = _id
)
