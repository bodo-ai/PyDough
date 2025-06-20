WITH _S1 AS (
  SELECT
    COUNT(*) AS AGG_0,
    SUM(sale_price) AS AGG_1,
    car_id AS CAR_ID
  FROM MAIN.SALES
  WHERE
    sale_date >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  GROUP BY
    car_id
)
SELECT
  COALESCE(_S1.AGG_0, 0) AS num_sales,
  CASE
    WHEN (
      NOT _S1.AGG_0 IS NULL AND _S1.AGG_0 > 0
    )
    THEN COALESCE(_S1.AGG_1, 0)
    ELSE NULL
  END AS total_revenue
FROM MAIN.CARS AS CARS
LEFT JOIN _S1 AS _S1
  ON CARS._id = _S1.CAR_ID
WHERE
  LOWER(CARS.make) LIKE '%toyota%'
