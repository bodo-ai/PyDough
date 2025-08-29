SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT LOWER(c_lname) IN ('lee', 'smith', 'rodriguez')
