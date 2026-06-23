WITH _t2 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  QUALIFY
    NOT is_in_inventory
    AND ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC NULLS FIRST) = 1
)
SELECT
  ANY_VALUE(cars.make) AS make,
  ANY_VALUE(cars.model) AS model,
  MAX(sales.sale_price) AS highest_sale_price
FROM main.cars AS cars
JOIN _t2 AS _t2
  ON _t2.car_id = cars._id
LEFT JOIN main.sales AS sales
  ON cars._id = sales.car_id
GROUP BY
  cars._id
ORDER BY
  3 DESC
