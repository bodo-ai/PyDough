SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  join_date > '2025-01-01'
