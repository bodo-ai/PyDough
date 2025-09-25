WITH _s1 AS (
  SELECT
    customer_id,
    MIN(transaction_date) AS min_transaction_date
  FROM bodo.retail.transactions
  GROUP BY
    1
)
SELECT
  ROUND(
    AVG(
      DATEDIFF(
        SECOND,
        CAST(_s1.min_transaction_date AS DATETIME),
        CAST(loyalty_members.join_date AS DATETIME)
      )
    ),
    2
  ) AS avg_secs
FROM bodo.retail.loyalty_members AS loyalty_members
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = loyalty_members.customer_id
