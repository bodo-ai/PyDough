SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  a_balance IN (46240000.0, 57760000.0)
