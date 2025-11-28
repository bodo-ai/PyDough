SELECT
  cars.make,
  cars.model,
  sales.sale_price
FROM main.sales AS sales
JOIN main.cars AS cars
  ON cars._id = sales.car_id
JOIN main.inventory_snapshots AS inventory_snapshots
  ON NOT inventory_snapshots.is_in_inventory
  AND cars._id = inventory_snapshots.car_id
  AND inventory_snapshots.snapshot_date = sales.sale_date
ORDER BY
  3 DESC
LIMIT 1
