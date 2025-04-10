WITH _t1 AS (
  SELECT
    _id,
    cost
  FROM main.cars
), _t4 AS (
  SELECT
    SUM(_t1.cost) AS agg_2,
    SUM(sales.sale_price) AS agg_0
  FROM main.sales AS sales
  LEFT JOIN _t1 AS _t1
    ON _t1._id = sales.car_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
), _t5 AS (
  SELECT
    SUM(_t3.cost) AS agg_1
  FROM main.sales AS sales
  JOIN _t1 AS _t3
    ON _t3._id = sales.car_id
  WHERE
    CAST(STRFTIME('%Y', sales.sale_date) AS INTEGER) = 2023
)
SELECT
  (
    CAST((
      COALESCE(_t4.agg_0, 0) - COALESCE(_t5.agg_1, 0)
    ) AS REAL) / COALESCE(_t4.agg_2, 0)
  ) * 100 AS GPM
FROM _t4 AS _t4
LEFT JOIN _t5 AS _t5
  ON TRUE
