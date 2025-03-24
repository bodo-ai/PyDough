SELECT
  transaction_type,
  num_customers,
  avg_shares
FROM (
  SELECT
    avg_shares,
    num_customers,
    ordering_2,
    transaction_type
  FROM (
    SELECT
      AVG(shares) AS avg_shares,
      COUNT(DISTINCT customer_id) AS num_customers,
      COUNT(DISTINCT customer_id) AS ordering_2,
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
          date_time <= DATE_STR_TO_DATE('2023-03-31')
        )
        AND (
          date_time >= DATE_STR_TO_DATE('2023-01-01')
        )
    )
    GROUP BY
      transaction_type
  )
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
ORDER BY
  ordering_2 DESC
