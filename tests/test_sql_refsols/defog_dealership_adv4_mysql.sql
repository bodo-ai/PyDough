WITH _s1 AS (
  SELECT
    car_id,
    sale_price
  FROM main.sales
  WHERE
    sale_date >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '30' DAY)
)
SELECT
  COALESCE(NULLIF(COUNT(_s1.car_id), 0), 0) AS num_sales,
  CASE
    WHEN (
      NOT NULLIF(COUNT(_s1.car_id), 0) IS NULL AND NULLIF(COUNT(_s1.car_id), 0) > 0
    )
    THEN COALESCE(SUM(_s1.sale_price), 0)
    ELSE NULL
  END AS total_revenue
FROM main.cars AS cars
LEFT JOIN _s1 AS _s1
  ON _s1.car_id = cars._id
WHERE
  LOWER(cars.make) LIKE '%toyota%'
GROUP BY
  cars._id
