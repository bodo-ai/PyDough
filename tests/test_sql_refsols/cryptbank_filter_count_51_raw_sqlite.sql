SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  IIF(SUBSTRING(LOWER(c_fname), 1, 1) IN ('q', 'r', 's'), LOWER(c_fname), LOWER(c_lname)) LIKE '%ee%'
