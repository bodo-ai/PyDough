WITH _s0 AS (
  SELECT
    sales.car_id AS car_id,
    sales.sale_date AS sale_date,
    sales.sale_price AS sale_price
  FROM main.sales AS sales
), _s1 AS (
  SELECT
    cars._id AS _id
  FROM main.cars AS cars
), _t0 AS (
  SELECT
    inventory_snapshots.car_id AS car_id,
    inventory_snapshots.is_in_inventory AS is_in_inventory,
    inventory_snapshots.snapshot_date AS snapshot_date
  FROM main.inventory_snapshots AS inventory_snapshots
  WHERE
    inventory_snapshots.is_in_inventory = 0
), _s2 AS (
  SELECT
    _t0.car_id AS car_id
  FROM _t0 AS _t0
  WHERE
    _s0.sale_date = _t0.snapshot_date
), _s3 AS (
  SELECT
    _s1._id AS _id
  FROM _s1 AS _s1
  JOIN _s2 AS _s2
    ON _s1._id = _s2.car_id
), _s4 AS (
  SELECT
    _s0.car_id AS car_id,
    _s0.sale_price AS sale_price
  FROM _s0 AS _s0
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM _s3 AS _s3
      WHERE
        _s0.car_id = _s3._id
    )
), _s5 AS (
  SELECT
    cars._id AS _id,
    cars.make AS make,
    cars.model AS model
  FROM main.cars AS cars
)
SELECT
  _s5.make AS make,
  _s5.model AS model,
  _s4.sale_price AS sale_price
FROM _s4 AS _s4
JOIN _s5 AS _s5
  ON _s4.car_id = _s5._id
ORDER BY
  sale_price DESC
LIMIT 1
