SELECT
  COUNT(_id) AS TSC
FROM (
  SELECT
    _id
  FROM (
    SELECT
      _id,
      sale_date
    FROM main.sales
  )
  WHERE
    CAST((JULIANDAY(DATE(DATETIME('now'), 'start of day')) - JULIANDAY(DATE(sale_date, 'start of day'))) AS INTEGER) <= 7
)
