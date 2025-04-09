WITH _s1 AS (
  SELECT
    _id,
    cost
  FROM main.cars
), _s4 AS (
  SELECT
    SUM(_s1.cost) AS agg_2,
    SUM(sales.sale_price) AS agg_0
  FROM main.sales AS sales
  LEFT JOIN _s1 AS _s1
    ON _s1._id = sales.car_id
  WHERE
    EXTRACT(YEAR FROM sales.sale_date) = 2023
), _s5 AS (
  SELECT
    SUM(_s3.cost) AS agg_1
  FROM main.sales AS sales
  JOIN _s1 AS _s3
    ON _s3._id = sales.car_id
  WHERE
    EXTRACT(YEAR FROM sales.sale_date) = 2023
)
SELECT
  (
    (
      COALESCE(_s4.agg_0, 0) - COALESCE(_s5.agg_1, 0)
    ) / COALESCE(_s4.agg_2, 0)
  ) * 100 AS GPM
FROM _s4 AS _s4
LEFT JOIN _s5 AS _s5
  ON TRUE
