WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END DESC, snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
)
SELECT
  ANY_VALUE(cars.make) AS make,
  ANY_VALUE(cars.model) AS model,
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
