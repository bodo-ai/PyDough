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
      ) AS _t3
      WHERE
        (
          date_time <= '2023-03-31'
        ) AND (
          date_time >= '2023-01-01'
        )
    ) AS _t2
    GROUP BY
      transaction_type
  ) AS _t1
  ORDER BY
    ordering_2 DESC
  LIMIT 3
) AS _t0
ORDER BY
  ordering_2 DESC
