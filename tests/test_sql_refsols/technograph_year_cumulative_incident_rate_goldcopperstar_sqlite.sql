WITH _s14 AS (
  SELECT
    MAX(pr_release) AS release_date
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _t6 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _t8 AS (
  SELECT
    pr_id AS _id,
    pr_name AS name
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s5 AS (
  SELECT
    COUNT() AS agg_3,
    _s0.calendar_day
  FROM _t6 AS _s0
  JOIN main.devices AS devices
    ON _s0.calendar_day = DATE(devices.de_purchase_ts, 'start of day')
  JOIN _t8 AS _t8
    ON _t8._id = devices.de_product_id
  GROUP BY
    _s0.calendar_day
), _s13 AS (
  SELECT
    COUNT() AS agg_6,
    _s6.calendar_day
  FROM _t6 AS _s6
  JOIN main.incidents AS incidents
    ON _s6.calendar_day = DATE(incidents.in_error_report_ts, 'start of day')
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t10
    ON _t10._id = devices.de_product_id
  GROUP BY
    _s6.calendar_day
), _s15 AS (
  SELECT
    SUM(_s5.agg_3) AS agg_5,
    SUM(_s13.agg_6) AS agg_8,
    CAST(STRFTIME('%Y', _t6.calendar_day) AS INTEGER) AS year
  FROM _t6 AS _t6
  LEFT JOIN _s5 AS _s5
    ON _s5.calendar_day = _t6.calendar_day
  LEFT JOIN _s13 AS _s13
    ON _s13.calendar_day = _t6.calendar_day
  GROUP BY
    CAST(STRFTIME('%Y', _t6.calendar_day) AS INTEGER)
), _t0 AS (
  SELECT
    COALESCE(_s15.agg_5, 0) AS bought,
    COALESCE(_s15.agg_8, 0) AS incidents,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s15.agg_5, 0) - LAG(COALESCE(_s15.agg_5, 0), 1) OVER (ORDER BY _s15.year)
        )
      ) AS REAL) / LAG(COALESCE(_s15.agg_5, 0), 1) OVER (ORDER BY _s15.year),
      2
    ) AS pct_bought_change,
    ROUND(
      CAST((
        100.0 * (
          COALESCE(_s15.agg_8, 0) - LAG(COALESCE(_s15.agg_8, 0), 1) OVER (ORDER BY _s15.year)
        )
      ) AS REAL) / LAG(COALESCE(_s15.agg_8, 0), 1) OVER (ORDER BY _s15.year),
      2
    ) AS pct_incident_change,
    ROUND(
      CAST(SUM(COALESCE(_s15.agg_8, 0)) OVER (ORDER BY _s15.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS REAL) / SUM(COALESCE(_s15.agg_5, 0)) OVER (ORDER BY _s15.year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    _s15.year - CAST(STRFTIME('%Y', _s14.release_date) AS INTEGER) AS years_since_release
  FROM _s14 AS _s14
  JOIN _s15 AS _s15
    ON _s15.year >= CAST(STRFTIME('%Y', _s14.release_date) AS INTEGER)
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
