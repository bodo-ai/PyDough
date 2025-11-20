WITH _t2 AS (
  SELECT
    in_device_id
  FROM main.incidents
), _s1 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s3 AS (
  SELECT
    devices.de_production_country_id,
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_nrows
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.in_device_id = devices.de_id
  GROUP BY
    1
), _s5 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s7 AS (
  SELECT
    devices.de_purchase_country_id,
    COUNT(*) AS n_rows,
    SUM(_s5.n_rows) AS sum_nrows
  FROM main.devices AS devices
  LEFT JOIN _s5 AS _s5
    ON _s5.in_device_id = devices.de_id
  GROUP BY
    1
), _s11 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s13 AS (
  SELECT
    users.us_country_id,
    COUNT(*) AS n_rows,
    SUM(_s11.n_rows) AS sum_nrows
  FROM main.users AS users
  JOIN main.devices AS devices
    ON devices.de_owner_id = users.us_id
  LEFT JOIN _s11 AS _s11
    ON _s11.in_device_id = devices.de_id
  GROUP BY
    1
)
SELECT
  countries.co_name AS country_name,
  ROUND(COALESCE(_s3.sum_nrows, 0) / _s3.n_rows, 2) AS made_ir,
  ROUND(COALESCE(_s7.sum_nrows, 0) / _s7.n_rows, 2) AS sold_ir,
  ROUND(COALESCE(_s13.sum_nrows, 0) / COALESCE(_s13.n_rows, 0), 2) AS user_ir
FROM main.countries AS countries
JOIN _s3 AS _s3
  ON _s3.de_production_country_id = countries.co_id
JOIN _s7 AS _s7
  ON _s7.de_purchase_country_id = countries.co_id
LEFT JOIN _s13 AS _s13
  ON _s13.us_country_id = countries.co_id
ORDER BY
  1 NULLS FIRST
