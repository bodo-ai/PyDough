SELECT
  COUNT(*) AS n
FROM bodo.retail.loyalty_members
WHERE
  join_date > '2025-01-01'
