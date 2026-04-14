SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  DATETIME(a_open_ts, '+123456789 seconds') <= '2020-09-20 08:30:00'
  AND DATETIME(a_open_ts, '+123456789 seconds') >= '2020-03-28 09:20:00'
