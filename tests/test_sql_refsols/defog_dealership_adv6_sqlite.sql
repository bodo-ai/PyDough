WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
), _s3 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
)
SELECT
  MAX(cars.make) AS make,
  MAX(cars.model) AS model,
  MAX(_s3.sale_price) AS highest_sale_price
FROM main.cars AS cars
JOIN _t AS _t
  ON NOT _t.is_in_inventory AND _t._w = 1 AND _t.car_id = cars._id
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = cars._id
GROUP BY
  _s3.car_id
ORDER BY
  3 DESC
