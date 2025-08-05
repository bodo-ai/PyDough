SELECT
  CURRENT_TIMESTAMP() AS ts_1,
  DATE_TRUNC('MONTH', CURRENT_TIMESTAMP()) AS ts_2,
  DATE_ADD(CURRENT_TIMESTAMP(), 12, 'HOUR') AS ts_3,
  DATE_ADD(DATE_TRUNC('YEAR', CURRENT_TIMESTAMP()), -1, 'DAY') AS ts_4,
  DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) AS ts_5,
  DATE_ADD(DATE_TRUNC('QUARTER', CURRENT_TIMESTAMP()), 1, 'DAY') AS ts_6,
  EXTRACT(YEAR FROM CAST(o_orderdate AS DATETIME)) AS year,
  EXTRACT(QUARTER FROM CAST(o_orderdate AS DATETIME)) AS qtr,
  EXTRACT(MONTH FROM CAST(o_orderdate AS DATETIME)) AS month,
  EXTRACT(DAY FROM CAST(o_orderdate AS DATETIME)) AS day,
  EXTRACT(HOUR FROM CAST(o_orderdate AS DATETIME)) AS hour,
  EXTRACT(MINUTE FROM CAST(o_orderdate AS DATETIME)) AS minute,
  EXTRACT(SECOND FROM CAST(o_orderdate AS DATETIME)) AS second,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), SECOND) AS seconds_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), MINUTE) AS minutes_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), HOUR) AS hours_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), DAY) AS days_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), WEEK) AS weeks_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), MONTH) AS months_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), QUARTER) AS qtrs_since,
  DATEDIFF(CAST('1992-01-01' AS DATE), CAST(o_orderdate AS DATETIME), YEAR) AS years_since,
  DAY_OF_WEEK(o_orderdate) AS day_of_week,
  CASE
    WHEN DAY_OF_WEEK(o_orderdate) = 0
    THEN 'Sunday'
    WHEN DAY_OF_WEEK(o_orderdate) = 1
    THEN 'Monday'
    WHEN DAY_OF_WEEK(o_orderdate) = 2
    THEN 'Tuesday'
    WHEN DAY_OF_WEEK(o_orderdate) = 3
    THEN 'Wednesday'
    WHEN DAY_OF_WEEK(o_orderdate) = 4
    THEN 'Thursday'
    WHEN DAY_OF_WEEK(o_orderdate) = 5
    THEN 'Friday'
    WHEN DAY_OF_WEEK(o_orderdate) = 6
    THEN 'Saturday'
  END AS day_name
FROM tpch.orders
