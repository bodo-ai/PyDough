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
    DATEDIFF(CURRENT_TIMESTAMP(), sale_date, DAY) <= 7
)
