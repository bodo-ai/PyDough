SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  (
    SUBSTRING(c_email, -1) || SUBSTRING(c_email, 1, LENGTH(c_email) - 1)
  ) LIKE '%.%@%mail%'
