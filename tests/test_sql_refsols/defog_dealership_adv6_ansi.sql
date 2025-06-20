WITH _t1 AS (
  SELECT
    car_id
  FROM main.inventory_snapshots
  QUALIFY
    NOT is_in_inventory
    AND ROW_NUMBER() OVER (PARTITION BY car_id ORDER BY snapshot_date DESC NULLS FIRST) = 1
), _s6 AS (
  SELECT
    MAX(sale_price) AS agg_0,
    car_id
  FROM main.sales
  GROUP BY
    car_id
)
SELECT
  _s0.make,
  _s0.model,
  _s6.agg_0 AS highest_sale_price
FROM main.cars AS _s0
JOIN _t1 AS _t1
  ON _s0._id = _t1.car_id
LEFT JOIN _s6 AS _s6
  ON _s0._id = _s6.car_id
ORDER BY
  _s6.agg_0 DESC
