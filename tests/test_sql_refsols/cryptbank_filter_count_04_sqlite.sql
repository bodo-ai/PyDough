SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_lname IN ('lee', 'smith', 'rodriguez')
