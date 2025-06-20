WITH _t0 AS (
  SELECT
    MAX(_s1.make) AS make,
    MAX(_s1.model) AS model,
    MAX(_s0.sale_price) AS sale_price
  FROM main.sales AS _s0
  JOIN main.cars AS _s1
    ON _s0.car_id = _s1._id
  JOIN main.inventory_snapshots AS _s4
    ON _s0.sale_date = _s4.snapshot_date
    AND _s1._id = _s4.car_id
    AND _s4.is_in_inventory = 0
  GROUP BY
    _s0._id,
    _s1._id
)
SELECT
  make,
  model,
  sale_price
FROM _t0
ORDER BY
  sale_price DESC
LIMIT 1
