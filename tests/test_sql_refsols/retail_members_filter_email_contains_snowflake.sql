SELECT
  COUNT(*) AS n
FROM bodo.retail.loyalty_members
WHERE
  NOT CONTAINS(email, 'mail')
