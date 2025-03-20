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
      CAST((JULIANDAY(DATE('now', 'start of day')) - JULIANDAY(DATE(join_date, 'start of day'))) AS INTEGER) <= 70
  )
    ON customer_id = _id
)
