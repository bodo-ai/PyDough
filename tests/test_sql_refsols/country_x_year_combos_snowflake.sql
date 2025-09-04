WITH _s3 AS (
  SELECT DISTINCT
    DATE_TRUNC('YEAR', CAST(calendar.ca_dt AS TIMESTAMP)) AS start_of_year
  FROM main.products AS products
  JOIN main.calendar AS calendar
    ON calendar.ca_dt < DATEADD(YEAR, 2, CAST(products.pr_release AS TIMESTAMP))
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
  NOT CONTAINS(countries.co_name, 'C')
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
