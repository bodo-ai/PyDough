WITH _s4 AS (
  SELECT
    COUNT(*) AS agg_9,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s6 AS (
  SELECT
    COUNT(*) AS agg_1,
    SUM(_s4.agg_9) AS agg_11,
    _s1.de_production_country_id AS factory_country_id
  FROM main.devices AS _s1
  LEFT JOIN _s4 AS _s4
    ON _s1.de_id = _s4.device_id
  GROUP BY
    _s1.de_production_country_id
), _s10 AS (
  SELECT
    COUNT(*) AS agg_12,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s12 AS (
  SELECT
    SUM(_s10.agg_12) AS agg_14,
    COUNT(*) AS agg_3,
    _s7.de_purchase_country_id AS store_country_id
  FROM main.devices AS _s7
  LEFT JOIN _s10 AS _s10
    ON _s10.device_id = _s7.de_id
  GROUP BY
    _s7.de_purchase_country_id
), _s19 AS (
  SELECT
    COUNT(*) AS agg_6,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s21 AS (
  SELECT
    COUNT(*) AS agg_5,
    SUM(_s19.agg_6) AS agg_8,
    _s13.us_country_id AS country_id
  FROM main.users AS _s13
  JOIN main.devices AS _s14
    ON _s13.us_id = _s14.de_owner_id
  LEFT JOIN _s19 AS _s19
    ON _s14.de_id = _s19.device_id
  GROUP BY
    _s13.us_country_id
)
SELECT
  _s0.co_name AS country_name,
  ROUND(CAST(COALESCE(_s6.agg_11, 0) AS REAL) / _s6.agg_1, 2) AS made_ir,
  ROUND(CAST(COALESCE(_s12.agg_14, 0) AS REAL) / _s12.agg_3, 2) AS sold_ir,
  ROUND(CAST(COALESCE(_s21.agg_8, 0) AS REAL) / COALESCE(_s21.agg_5, 0), 2) AS user_ir
FROM main.countries AS _s0
JOIN _s6 AS _s6
  ON _s0.co_id = _s6.factory_country_id
JOIN _s12 AS _s12
  ON _s0.co_id = _s12.store_country_id
LEFT JOIN _s21 AS _s21
  ON _s0.co_id = _s21.country_id
ORDER BY
  country_name
