SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth IN ('2637-10-01', '1403-11-22')
