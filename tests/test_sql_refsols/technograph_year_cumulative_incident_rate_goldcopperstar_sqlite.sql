WITH _s0 AS (
  SELECT
    MAX(pr_release) AS release_date
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _t6 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s1 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', calendar_day) AS INTEGER) AS year
  FROM _t6
), _t9 AS (
  SELECT
    pr_id AS _id,
    pr_name AS name
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s7 AS (
  SELECT
    COUNT() AS agg_1,
    CAST(STRFTIME('%Y', _t8.calendar_day) AS INTEGER) AS year
  FROM _t6 AS _t8
  JOIN main.devices AS devices
    ON _t8.calendar_day = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t9 AS _t9
    ON _t9._id = devices.de_product_id
  GROUP BY
    CAST(STRFTIME('%Y', _t8.calendar_day) AS INTEGER)
), _s15 AS (
  SELECT
    COUNT() AS agg_2,
    CAST(STRFTIME('%Y', _t11.calendar_day) AS INTEGER) AS year
  FROM _t6 AS _t11
  JOIN main.incidents AS incidents
    ON _t11.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t9 AS _t12
    ON _t12._id = devices.de_product_id
  GROUP BY
    CAST(STRFTIME('%Y', _t11.calendar_day) AS INTEGER)
), _t0 AS (
  SELECT
    COALESCE(_s7.agg_1, 0) AS bought,
    COALESCE(_s15.agg_2, 0) AS incidents,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s7.agg_1, 0) - LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s1.year)
        )
      ) AS REAL) / LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s1.year),
      2
    ) AS pct_bought_change,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s15.agg_2, 0) - LAG(COALESCE(_s15.agg_2, 0), 1) OVER (ORDER BY _s1.year)
        )
      ) AS REAL) / LAG(COALESCE(_s15.agg_2, 0), 1) OVER (ORDER BY _s1.year),
      2
    ) AS pct_incident_change,
    ROUND(
      CAST(SUM(COALESCE(_s15.agg_2, 0)) OVER (ORDER BY _s1.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s7.agg_1, 0)) OVER (ORDER BY _s1.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    _s1.year - CAST(STRFTIME('%Y', _s0.release_date) AS INTEGER) AS years_since_release
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s1.year >= CAST(STRFTIME('%Y', _s0.release_date) AS INTEGER)
  LEFT JOIN _s7 AS _s7
    ON _s1.year = _s7.year
  LEFT JOIN _s15 AS _s15
    ON _s1.year = _s15.year
)
SELECT
  years_since_release,
  cum_ir,
  pct_bought_change,
  pct_incident_change,
  bought,
  incidents
FROM _t0
ORDER BY
  years_since_release
