SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  ENDSWITH(first_name, 'e') OR ENDSWITH(last_name, 'e')
