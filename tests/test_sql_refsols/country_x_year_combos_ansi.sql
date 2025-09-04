WITH _s3 AS (
  SELECT DISTINCT
    DATE_TRUNC('YEAR', CAST(calendar.ca_dt AS TIMESTAMP)) AS start_of_year
  FROM main.products AS products
  JOIN main.calendar AS calendar
    ON calendar.ca_dt < DATE_ADD(CAST(products.pr_release AS TIMESTAMP), 2, 'YEAR')
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
