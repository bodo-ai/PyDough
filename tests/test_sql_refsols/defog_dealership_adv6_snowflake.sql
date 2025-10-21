WITH _t2 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  QUALIFY
    NOT is_in_inventory
    AND ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC) = 1
), _s3 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
)
SELECT
  ANY_VALUE(cars.make) AS make,
  ANY_VALUE(cars.model) AS model,
  MAX(_s3.sale_price) AS highest_sale_price
FROM main.cars AS cars
JOIN _t2 AS _t2
  ON _t2.car_id = cars._id
LEFT JOIN _s3 AS _s3
  ON _s3.car_id = cars._id
GROUP BY
  cars._id
ORDER BY
  3 DESC NULLS LAST
