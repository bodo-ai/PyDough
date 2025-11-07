WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
)
SELECT
  MAX(cars.make) AS make,
  MAX(cars.model) AS model,
  MAX(sales.sale_price) AS highest_sale_price
FROM main.cars AS cars
JOIN _t AS _t
  ON NOT _t.is_in_inventory AND _t._w = 1 AND _t.car_id = cars._id
LEFT JOIN main.sales AS sales
  ON cars._id = sales.car_id
GROUP BY
  cars._id
ORDER BY
  3 DESC
