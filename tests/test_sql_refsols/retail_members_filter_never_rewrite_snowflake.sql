SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  join_date > '2026-01-01'
