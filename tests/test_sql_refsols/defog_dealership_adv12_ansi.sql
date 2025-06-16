WITH _t0 AS (
  SELECT
    inventory_snapshots.car_id AS car_id,
    inventory_snapshots.is_in_inventory AS is_in_inventory,
    inventory_snapshots.snapshot_date AS snapshot_date
  FROM main.inventory_snapshots AS inventory_snapshots
), _s3 AS (
  SELECT
    _t0.car_id AS car_id
  FROM _t0 AS _t0
  WHERE
    _s0.sale_date = _t0.snapshot_date AND _t0.is_in_inventory = 0
), _s1 AS (
  SELECT
    sales.car_id AS car_id,
    sales.sale_date AS sale_date,
    sales.sale_price AS sale_price
  FROM main.sales AS sales
), _s2 AS (
  SELECT
    cars._id AS _id,
    cars.make AS make,
    cars.model AS model
  FROM main.cars AS cars
), _s0 AS (
  SELECT
    _s2._id AS _id_1,
    _s2.make AS make,
    _s2.model AS model,
    _s1.sale_date AS sale_date,
    _s1.sale_price AS sale_price
  FROM _s1 AS _s1
  JOIN _s2 AS _s2
    ON _s1.car_id = _s2._id
)
SELECT
  _s0.make AS make,
  _s0.model AS model,
  _s0.sale_price AS sale_price
FROM _s0 AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _s3 AS _s3
    WHERE
      _s0._id_1 = _s3.car_id
  )
ORDER BY
  sale_price DESC
LIMIT 1
