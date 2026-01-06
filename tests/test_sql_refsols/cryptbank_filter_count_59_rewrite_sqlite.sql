SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_fname IN ('CAROL', 'EMILY', 'ISABEL', 'LUKE', 'SOPHIA')
