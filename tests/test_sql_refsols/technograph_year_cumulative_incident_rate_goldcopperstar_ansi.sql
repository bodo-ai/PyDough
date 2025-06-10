WITH _s14 AS (
  SELECT
    ANY_VALUE(pr_release) AS release_date
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s0 AS (
  SELECT
    ca_dt AS calendar_day,
    ca_dt AS key_0
  FROM main.calendar
), _s2 AS (
  SELECT
    COUNT() AS agg_3,
    _s0.calendar_day,
    incidents.in_device_id AS device_id
  FROM _s0 AS _s0
  JOIN main.incidents AS incidents
    ON _s0.key_0 = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    _s0.calendar_day,
    incidents.in_device_id
), _s4 AS (
  SELECT
    SUM(_s2.agg_3) AS agg_3,
    _s2.calendar_day,
    devices.de_product_id AS product_id
  FROM _s2 AS _s2
  JOIN main.devices AS devices
    ON _s2.device_id = devices.de_id
  GROUP BY
    _s2.calendar_day,
    devices.de_product_id
), _t10 AS (
  SELECT
    pr_id AS _id,
    pr_name AS name
  FROM main.products
), _s7 AS (
  SELECT
    SUM(_s4.agg_3) AS agg_3,
    _s4.calendar_day
  FROM _s4 AS _s4
  JOIN _t10 AS _t10
    ON _s4.product_id = _t10._id AND _t10.name = 'GoldCopper-Star'
  GROUP BY
    _s4.calendar_day
), _s10 AS (
  SELECT
    COUNT() AS agg_6,
    _s8.calendar_day,
    devices.de_product_id AS product_id
  FROM _s0 AS _s8
  JOIN main.devices AS devices
    ON _s8.key_0 = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    _s8.calendar_day,
    devices.de_product_id
), _s13 AS (
  SELECT
    SUM(_s10.agg_6) AS agg_6,
    _s10.calendar_day
  FROM _s10 AS _s10
  JOIN _t10 AS _t13
    ON _s10.product_id = _t13._id AND _t13.name = 'GoldCopper-Star'
  GROUP BY
    _s10.calendar_day
), _s15 AS (
  SELECT
    SUM(_s7.agg_3) AS agg_5,
    SUM(_s13.agg_6) AS agg_8,
    EXTRACT(YEAR FROM calendar.ca_dt) AS year
  FROM main.calendar AS calendar
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = calendar.ca_dt
  LEFT JOIN _s13 AS _s13
    ON _s13.calendar_day = calendar.ca_dt
  GROUP BY
    EXTRACT(YEAR FROM calendar.ca_dt)
), _t0 AS (
  SELECT
    COALESCE(_s15.agg_8, 0) AS bought,
    ROUND(
      SUM(COALESCE(_s15.agg_5, 0)) OVER (ORDER BY _s15.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s15.agg_8, 0)) OVER (ORDER BY _s15.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(_s15.agg_5, 0) AS incidents,
    ROUND(
      (
        100.0 * (
          COALESCE(_s15.agg_8, 0) - LAG(COALESCE(_s15.agg_8, 0), 1) OVER (ORDER BY _s15.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s15.agg_8, 0), 1) OVER (ORDER BY _s15.year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(_s15.agg_5, 0) - LAG(COALESCE(_s15.agg_5, 0), 1) OVER (ORDER BY _s15.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s15.agg_5, 0), 1) OVER (ORDER BY _s15.year NULLS LAST),
      2
    ) AS pct_incident_change,
    _s15.year - EXTRACT(YEAR FROM _s14.release_date) AS years_since_release
  FROM _s14 AS _s14
  CROSS JOIN _s15 AS _s15
  WHERE
    _s15.year >= EXTRACT(YEAR FROM _s14.release_date)
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
