WITH _s0 AS (
  SELECT
    co_id,
    co_name
  FROM main.COUNTRIES
), _s2 AS (
  SELECT
    co_id
  FROM main.COUNTRIES
), _s7 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.INCIDENTS
  GROUP BY
    1
), _s9 AS (
  SELECT
    _s3.co_id AS _id_3,
    _s2.co_id,
    COUNT(*) AS n_rows,
    SUM(_s7.n_rows) AS sum_n_rows
  FROM _s2 AS _s2
  CROSS JOIN _s2 AS _s3
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_production_country_id = _s2.co_id
    AND DEVICES.de_purchase_country_id = _s3.co_id
  LEFT JOIN _s7 AS _s7
    ON DEVICES.de_id = _s7.in_device_id
  GROUP BY
    1,
    2
)
SELECT
  _s0.co_name AS factory_country,
  _s1.co_name AS purchase_country,
  ROUND(COALESCE(_s9.sum_n_rows, 0) / COALESCE(_s9.n_rows, 0), 2) AS ir
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.co_id = _s9.co_id AND _s1.co_id = _s9._id_3
ORDER BY
  3 DESC
LIMIT 5
