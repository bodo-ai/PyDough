WITH _t1 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    NOT co_name LIKE '%C%'
), _s7 AS (
  SELECT DISTINCT
    DATE_TRUNC('YEAR', CAST(calendar.ca_dt AS TIMESTAMP)) AS start_of_year,
    _t4.co_name
  FROM _t1 AS _t4
  JOIN main.products AS products
    ON products.pr_name = 'AmethystCopper-I'
  JOIN main.calendar AS calendar
    ON calendar.ca_dt < DATE_ADD(CAST(products.pr_release AS TIMESTAMP), 2, 'YEAR')
    AND calendar.ca_dt >= products.pr_release
)
SELECT
  _t1.co_name AS country_name,
  _s7.start_of_year
FROM _t1 AS _t1
JOIN main.products AS products
  ON products.pr_name = 'AmethystCopper-I'
LEFT JOIN _s7 AS _s7
  ON _s7.co_name = _t1.co_name
ORDER BY
  1,
  2
