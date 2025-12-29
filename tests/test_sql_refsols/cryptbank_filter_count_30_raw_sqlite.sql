SELECT
  COUNT(*) AS n
FROM crbnk.customers
WHERE
  (
    CAST(STRFTIME('%Y', DATE(c_birthday, '+472 days')) AS INTEGER) - 2
  ) IN (1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993)
  AND (
    CAST(STRFTIME('%m', DATE(c_birthday, '+472 days')) AS INTEGER) + 1
  ) IN (2, 4, 6, 8, 10, 12)
