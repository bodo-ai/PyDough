SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_lname IN (UPPER('lee'), UPPER('smith'), UPPER('rodriguez'))
