WITH _s0 AS (
  SELECT
    co_id,
    co_name
  FROM main.countries
), _s2 AS (
  SELECT
    co_id
  FROM main.countries
), _t1 AS (
  SELECT
    MAX(_s3.co_id) AS anything__id_3,
    MAX(_s2.co_id) AS anything_co_id,
    COUNT(incidents.in_device_id) AS count_in_device_id
  FROM _s2 AS _s2
  CROSS JOIN _s2 AS _s3
  JOIN main.devices AS devices
    ON _s2.co_id = devices.de_production_country_id
    AND _s3.co_id = devices.de_purchase_country_id
  LEFT JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    devices.de_id
), _s9 AS (
  SELECT
    anything__id_3,
    anything_co_id,
    COUNT(*) AS n_rows,
    SUM(NULLIF(count_in_device_id, 0)) AS sum_n_rows
  FROM _t1
  GROUP BY
    1,
    2
)
SELECT
  _s0.co_name AS factory_country,
  _s1.co_name AS purchase_country,
  ROUND(
    CAST(CAST(COALESCE(_s9.sum_n_rows, 0) AS DOUBLE PRECISION) / COALESCE(_s9.n_rows, 0) AS DECIMAL),
    2
  ) AS ir
FROM _s0 AS _s0
CROSS JOIN _s0 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.co_id = _s9.anything_co_id AND _s1.co_id = _s9.anything__id_3
ORDER BY
  3 DESC NULLS LAST
LIMIT 5
