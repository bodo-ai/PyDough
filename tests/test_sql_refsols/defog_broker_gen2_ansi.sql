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
      DATEDIFF('now', join_date, DAY) <= 70
  )
    ON customer_id = _id
)
