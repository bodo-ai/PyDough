SELECT
  AVG(sale_price) AS ASP
FROM (
  SELECT
    sale_price
  FROM (
    SELECT
      sale_date,
      sale_price
    FROM main.sales
  )
  WHERE
    (
      sale_date <= '2023-03-31'
    ) AND (
      sale_date >= '2023-01-01'
    )
)
