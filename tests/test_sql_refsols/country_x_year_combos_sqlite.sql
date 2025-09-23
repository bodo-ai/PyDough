WITH _t1 AS (
  SELECT
    co_name
  FROM main.countries
  WHERE
    NOT co_name LIKE '%C%'
), _s5 AS (
  SELECT DISTINCT
    DATE(calendar.ca_dt, 'start of year') AS start_of_year,
    _t3.co_name
  FROM _t1 AS _t3
  JOIN main.products AS products
    ON products.pr_name = 'AmethystCopper-I'
  JOIN main.calendar AS calendar
    ON calendar.ca_dt < DATETIME(products.pr_release, '2 year')
    AND calendar.ca_dt >= products.pr_release
)
SELECT
  _t1.co_name AS country_name,
  _s5.start_of_year
FROM _t1 AS _t1
JOIN _s5 AS _s5
  ON _s5.co_name = _t1.co_name
ORDER BY
  1,
  2
