WITH _t1 AS (
  SELECT
    cars._id AS _id_1,
    ANY_VALUE(sales.car_id) AS anything_carid,
    ANY_VALUE(cars.make) AS anything_make,
    ANY_VALUE(cars.model) AS anything_model,
    ANY_VALUE(sales.sale_price) AS anything_saleprice
  FROM main.sales AS sales
  JOIN main.cars AS cars
    ON cars._id = sales.car_id
  JOIN main.inventory_snapshots AS inventory_snapshots
    ON NOT inventory_snapshots.is_in_inventory
    AND cars._id = inventory_snapshots.car_id
    AND inventory_snapshots.snapshot_date = sales.sale_date
  GROUP BY
    sales._id,
    1
)
SELECT
  anything_make AS make,
  anything_model AS model,
  anything_saleprice AS sale_price
FROM _t1
WHERE
  _id_1 = anything_carid
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
