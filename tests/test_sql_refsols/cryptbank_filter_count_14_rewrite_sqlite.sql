SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_fname IN ('ALICE', 'GRACE', 'LUKE', 'QUEENIE') OR c_lname IN ('LEE', 'MOORE')
