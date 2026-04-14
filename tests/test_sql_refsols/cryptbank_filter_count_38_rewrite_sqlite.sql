SELECT
  COUNT(*) AS n
FROM crbnk.accounts
WHERE
  a_open_ts IN ('2018-03-15 10:36:51', '2018-01-02 12:26:51')
