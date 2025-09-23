WITH _t1 AS (
  SELECT
    co_name
  FROM main.COUNTRIES
  WHERE
    NOT co_name LIKE '%C%'
), _s5 AS (
  SELECT DISTINCT
    STR_TO_DATE(CONCAT(YEAR(CAST(CALENDAR.ca_dt AS DATETIME)), ' 1 1'), '%Y %c %e') AS start_of_year,
    _t3.co_name
  FROM _t1 AS _t3
  JOIN main.PRODUCTS AS PRODUCTS
    ON PRODUCTS.pr_name = 'AmethystCopper-I'
  JOIN main.CALENDAR AS CALENDAR
    ON CALENDAR.ca_dt < DATE_ADD(CAST(PRODUCTS.pr_release AS DATETIME), INTERVAL '2' YEAR)
    AND CALENDAR.ca_dt >= PRODUCTS.pr_release
)
SELECT
  _t1.co_name COLLATE utf8mb4_bin AS country_name,
  _s5.start_of_year
FROM _t1 AS _t1
JOIN _s5 AS _s5
  ON _s5.co_name = _t1.co_name
ORDER BY
  1,
  2
