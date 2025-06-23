WITH _s0 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
), _s2 AS (
  SELECT
    co_id
  FROM main.countries
), _s7 AS (
  SELECT
    in_device_id AS device_id,
    COUNT(*) AS n_rows
  FROM main.incidents
  GROUP BY
    in_device_id
), _s9 AS (
  SELECT
    _s2.co_id AS _id,
    COUNT(*) AS n_rows,
    SUM(_s7.n_rows) AS sum_n_rows,
    _s3.co_id AS _id_3
  FROM _s2 AS _s2
  CROSS JOIN _s2 AS _s3
  JOIN main.devices AS devices
    ON _s2.co_id = devices.de_production_country_id
    AND _s3.co_id = devices.de_purchase_country_id
  LEFT JOIN _s7 AS _s7
    ON _s7.device_id = devices.de_id
  GROUP BY
    _s2.co_id,
    _s3.co_id
)
SELECT
  _s0.co_name AS factory_country,
  _s1.co_name AS purchase_country,
  ROUND(CAST((
    1.0 * COALESCE(_s9.sum_n_rows, 0)
  ) AS REAL) / COALESCE(_s9.n_rows, 0), 2) AS ir
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.co_id = _s9._id AND _s1.co_id = _s9._id_3
ORDER BY
  ir DESC
LIMIT 5
