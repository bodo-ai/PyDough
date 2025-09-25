SELECT
  COUNT(*) AS n
FROM bodo.retail.loyalty_members
WHERE
  loyalty_tier = 'Platinum'
