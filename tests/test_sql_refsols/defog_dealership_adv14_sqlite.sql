SELECT
  COUNT() AS TSC
FROM main.sales
WHERE
  CAST((
    JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))
  ) AS INTEGER) <= 7
