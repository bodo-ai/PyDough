SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  loyalty_tier = 'Platinum'
