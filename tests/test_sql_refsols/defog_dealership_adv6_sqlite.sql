WITH _t AS (
  SELECT
    car_id,
    is_in_inventory,
    ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC) AS _w
  FROM main.inventory_snapshots
), _s3 AS (
  SELECT
    MAX(sale_price) AS max_sale_price,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  cars.make,
  cars.model,
  _s3.max_sale_price AS highest_sale_price
FROM main.cars AS cars
JOIN _t AS _t
  ON NOT _t.is_in_inventory AND _t._w = 1 AND _t.car_id = cars._id
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = cars._id
ORDER BY
  _s3.max_sale_price DESC
