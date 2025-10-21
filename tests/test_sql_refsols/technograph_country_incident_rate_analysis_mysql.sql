WITH _t2 AS (
  SELECT
    in_device_id
  FROM main.INCIDENTS
), _s1 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM _t2
  GROUP BY
    1
), _s3 AS (
  SELECT
    DEVICES.de_production_country_id,
    COUNT(*) AS n_rows,
    SUM(_s1.n_rows) AS sum_n_rows
  FROM main.DEVICES AS DEVICES
  LEFT JOIN _s1 AS _s1
    ON DEVICES.de_id = _s1.in_device_id
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
    DEVICES.de_purchase_country_id,
    COUNT(*) AS n_rows,
    SUM(_s5.n_rows) AS sum_n_rows
  FROM main.DEVICES AS DEVICES
  LEFT JOIN _s5 AS _s5
    ON DEVICES.de_id = _s5.in_device_id
  GROUP BY
    1
), _t5 AS (
  SELECT
    _s11.in_device_id,
    ANY_VALUE(USERS.us_country_id) AS anything_us_country_id,
    COUNT(*) AS n_rows
  FROM main.USERS AS USERS
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_owner_id = USERS.us_id
  LEFT JOIN _t2 AS _s11
    ON DEVICES.de_id = _s11.in_device_id
  GROUP BY
    1
), _s13 AS (
  SELECT
    anything_us_country_id,
    COUNT(*) AS n_rows,
    SUM(n_rows * CASE WHEN NOT in_device_id IS NULL THEN 1 ELSE 0 END) AS sum_n_rows
  FROM _t5
  GROUP BY
    1
)
SELECT
  COUNTRIES.co_name COLLATE utf8mb4_bin AS country_name,
  ROUND(COALESCE(_s3.sum_n_rows, 0) / _s3.n_rows, 2) AS made_ir,
  ROUND(COALESCE(_s7.sum_n_rows, 0) / _s7.n_rows, 2) AS sold_ir,
  ROUND(COALESCE(_s13.sum_n_rows, 0) / COALESCE(_s13.n_rows, 0), 2) AS user_ir
FROM main.COUNTRIES AS COUNTRIES
JOIN _s3 AS _s3
  ON COUNTRIES.co_id = _s3.de_production_country_id
JOIN _s7 AS _s7
  ON COUNTRIES.co_id = _s7.de_purchase_country_id
LEFT JOIN _s13 AS _s13
  ON COUNTRIES.co_id = _s13.anything_us_country_id
ORDER BY
  1
