SELECT
  transaction_type,
  num_customers,
  avg_shares
FROM (
  SELECT
    AVG(shares) AS avg_shares,
    COUNT(DISTINCT customer_id) AS num_customers,
    transaction_type
  FROM (
    SELECT
      customer_id,
      shares,
      transaction_type
    FROM (
      SELECT
        sbTxCustId AS customer_id,
        sbTxDateTime AS date_time,
        sbTxShares AS shares,
        sbTxType AS transaction_type
      FROM main.sbTransaction
    )
    WHERE
      (
        date_time <= CAST('2023-03-31' AS DATE)
      )
      AND (
        date_time >= CAST('2023-01-01' AS DATE)
      )
  )
  GROUP BY
    transaction_type
)
ORDER BY
  num_customers DESC
LIMIT 3
