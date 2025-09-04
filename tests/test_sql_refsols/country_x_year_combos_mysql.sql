WITH _s3 AS (
  SELECT DISTINCT
    STR_TO_DATE(CONCAT(YEAR(CAST(CALENDAR.ca_dt AS DATETIME)), ' 1 1'), '%Y %c %e') AS start_of_year
  FROM main.PRODUCTS AS PRODUCTS
  JOIN main.CALENDAR AS CALENDAR
    ON CALENDAR.ca_dt < DATE_ADD(CAST(PRODUCTS.pr_release AS DATETIME), INTERVAL '2' YEAR)
    AND CALENDAR.ca_dt >= PRODUCTS.pr_release
  WHERE
    PRODUCTS.pr_name = 'AmethystCopper-I'
)
SELECT
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country_name,
  _s3.start_of_year
FROM main.COUNTRIES AS COUNTRIES
CROSS JOIN _s3 AS _s3
WHERE
  NOT COUNTRIES.co_name LIKE '%C%'
ORDER BY
  1,
  2
