SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  NOT c_lname IN (UPPER('lee'), UPPER('smith'), UPPER('rodriguez'))
