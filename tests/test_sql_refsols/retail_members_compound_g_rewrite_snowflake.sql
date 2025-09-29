SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  DAY(CAST(date_of_birth AS TIMESTAMP)) <= 13
  AND DAY(CAST(date_of_birth AS TIMESTAMP)) > 3
  AND MONTH(CAST(date_of_birth AS TIMESTAMP)) IN (1, 2, 5, 10, 12)
  AND YEAR(CAST(date_of_birth AS TIMESTAMP)) IN (1960, 1970, 1980, 1990, 2000)
