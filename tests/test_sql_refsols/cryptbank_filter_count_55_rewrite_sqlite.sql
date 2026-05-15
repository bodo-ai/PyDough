SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  c_fname IN ('ALICE', 'CAROL', 'FRANK', 'GRACE', 'JAMES', 'KAREN', 'MARIA', 'OLIVIA')
