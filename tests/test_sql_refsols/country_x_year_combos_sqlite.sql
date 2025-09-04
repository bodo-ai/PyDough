WITH _s3 AS (
  SELECT DISTINCT
    DATE(calendar.ca_dt, 'start of year') AS start_of_year
  FROM main.products AS products
  JOIN main.calendar AS calendar
    ON calendar.ca_dt < DATETIME(products.pr_release, '2 year')
    AND calendar.ca_dt >= products.pr_release
  WHERE
    products.pr_name = 'AmethystCopper-I'
)
SELECT
  countries.co_name AS country_name,
  _s3.start_of_year
FROM main.countries AS countries
CROSS JOIN _s3 AS _s3
WHERE
  NOT countries.co_name LIKE '%C%'
ORDER BY
  1,
  2
