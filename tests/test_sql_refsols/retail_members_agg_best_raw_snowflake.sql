WITH _t1 AS (
  SELECT
    protected_loyalty_members.first_name,
    protected_loyalty_members.last_name,
    transactions.store_location,
    transactions.total_amount
  FROM bodo.retail.transactions AS transactions
  JOIN bodo.retail.protected_loyalty_members AS protected_loyalty_members
    ON protected_loyalty_members.customer_id = transactions.customer_id
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY PTY_UNPROTECT_ADDRESS(store_location) ORDER BY transactions.total_amount DESC) = 1
)
SELECT
  PTY_UNPROTECT_ADDRESS(store_location) AS store_location,
  total_amount,
  CONCAT_WS(' ', PTY_UNPROTECT(first_name, 'deName'), PTY_UNPROTECT_NAME(last_name)) AS name
FROM _t1
ORDER BY
  2 DESC NULLS LAST
LIMIT 5
