WITH _t2 AS (
  SELECT
    in_device_id
  FROM main.INCIDENTS
), _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM _t2
  GROUP BY
    in_device_id
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_n_rows,
    DEVICES.de_production_country_id
  FROM main.DEVICES AS DEVICES
  LEFT JOIN _s1 AS _s1
    ON DEVICES.de_id = _s1.in_device_id
  GROUP BY
    DEVICES.de_production_country_id
), _s5 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM _t2
  GROUP BY
    in_device_id
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s5.n_rows) AS sum_n_rows,
    DEVICES.de_purchase_country_id
  FROM main.DEVICES AS DEVICES
  LEFT JOIN _s5 AS _s5
    ON DEVICES.de_id = _s5.in_device_id
  GROUP BY
    DEVICES.de_purchase_country_id
), _s11 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM _t2
  GROUP BY
    in_device_id
), _s13 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(_s11.n_rows) AS sum_n_rows,
    USERS.us_country_id
  FROM main.USERS AS USERS
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_owner_id = USERS.us_id
  LEFT JOIN _s11 AS _s11
    ON DEVICES.de_id = _s11.in_device_id
  GROUP BY
    USERS.us_country_id
)
SELECT
  COUNTRIES.co_name AS country_name,
  ROUND(COALESCE(_s3.sum_n_rows, 0) / _s3.n_rows, 2) AS made_ir,
  ROUND(COALESCE(_s7.sum_n_rows, 0) / _s7.n_rows, 2) AS sold_ir,
  ROUND(COALESCE(_s13.sum_n_rows, 0) / COALESCE(_s13.n_rows, 0), 2) AS user_ir
FROM main.COUNTRIES AS COUNTRIES
JOIN _s3 AS _s3
  ON COUNTRIES.co_id = _s3.de_production_country_id
JOIN _s7 AS _s7
  ON COUNTRIES.co_id = _s7.de_purchase_country_id
LEFT JOIN _s13 AS _s13
  ON COUNTRIES.co_id = _s13.us_country_id
ORDER BY
  COUNTRIES.co_name COLLATE utf8mb4_bin
