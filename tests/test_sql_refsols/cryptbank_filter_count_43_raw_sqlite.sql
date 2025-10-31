SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  CONCAT_WS(' ', LOWER(c_fname), LOWER(c_lname)) = 'olivia anderson'
