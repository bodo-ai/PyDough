SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  IIF(c_fname IN ('QUEENIE', 'ROBERT', 'SOPHIA'), LOWER(c_fname), LOWER(c_lname)) LIKE '%ee%'
