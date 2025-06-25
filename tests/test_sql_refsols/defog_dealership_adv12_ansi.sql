SELECT
  ANY_VALUE(cars.make) AS make,
  ANY_VALUE(cars.model) AS model,
  ANY_VALUE(sales.sale_price) AS sale_price
FROM main.sales AS sales
JOIN main.cars AS cars
  ON cars._id = sales.car_id
JOIN main.inventory_snapshots AS inventory_snapshots
  ON cars._id = inventory_snapshots.car_id
  AND inventory_snapshots.is_in_inventory = 0
  AND inventory_snapshots.snapshot_date = sales.sale_date
GROUP BY
  sales._id,
  cars._id
ORDER BY
  sale_price DESC
LIMIT 1
