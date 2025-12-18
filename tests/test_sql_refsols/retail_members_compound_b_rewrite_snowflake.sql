SELECT
  COUNT(*) AS n
FROM bodo.retail.protected_loyalty_members
WHERE
  date_of_birth = '1622-10-03' AND last_name <> PTY_PROTECT('Smith', 'deName')
