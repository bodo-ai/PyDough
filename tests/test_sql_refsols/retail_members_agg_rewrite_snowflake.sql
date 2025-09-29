WITH _s1 AS (
  SELECT
    customer_id,
    MIN(PTY_UNPROTECT_TS(transaction_date)) AS min_unmask_transaction_date
  FROM bodo.retail.transactions
  GROUP BY
    1
)
SELECT
  ROUND(
    AVG(
      DATEDIFF(
        SECOND,
        CAST(protected_loyalty_members.join_date AS DATETIME),
        CAST(_s1.min_unmask_transaction_date AS DATETIME)
      )
    ),
    2
  ) AS avg_secs
FROM bodo.retail.protected_loyalty_members AS protected_loyalty_members
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = protected_loyalty_members.customer_id
