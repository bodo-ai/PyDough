WITH _s2 AS (
  SELECT
    SUM(sale_price) AS agg_0,
    car_id
  FROM main.sales
  WHERE
    EXTRACT(YEAR FROM sale_date) = 2023
  GROUP BY
    car_id
), _t0 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0,
    SUM(_s1.cost) AS agg_1
  FROM _s2 AS _s2
  JOIN main.cars AS _s1
    ON _s1._id = _s2.car_id
)
SELECT
  (
    (
      COALESCE(agg_0, 0) - COALESCE(agg_1, 0)
    ) / COALESCE(agg_1, 0)
  ) * 100 AS GPM
FROM _t0
