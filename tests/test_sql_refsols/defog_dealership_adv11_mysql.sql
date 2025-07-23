SELECT
  (
    (
      COALESCE(SUM(sales.sale_price), 0) - COALESCE(SUM(cars.cost), 0)
    ) / COALESCE(SUM(cars.cost), 0)
  ) * 100 AS GPM
FROM main.sales AS sales
JOIN main.cars AS cars
  ON cars._id = sales.car_id
WHERE
  YEAR(sales.sale_date) = 2023
