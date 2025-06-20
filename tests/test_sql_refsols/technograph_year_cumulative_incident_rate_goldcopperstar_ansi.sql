WITH _s14 AS (
  SELECT
    ANY_VALUE(pr_release) AS release_date
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
), _s7 AS (
  SELECT
    COUNT(*) AS agg_3,
    _s0.calendar_day
  FROM _t6 AS _s0
  JOIN main.incidents AS incidents
    ON _s0.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t8 AS _t8
    ON _t8._id = devices.de_product_id AND _t8.name = 'GoldCopper-Star'
  GROUP BY
    _s0.calendar_day
), _s13 AS (
  SELECT
    COUNT(*) AS agg_6,
    _s8.calendar_day
  FROM _t6 AS _s8
  JOIN main.devices AS devices
    ON _s8.calendar_day = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t8 AS _t10
    ON _t10._id = devices.de_product_id AND _t10.name = 'GoldCopper-Star'
  GROUP BY
    _s8.calendar_day
), _s15 AS (
  SELECT
    SUM(_s7.agg_3) AS agg_5,
    SUM(_s13.agg_6) AS agg_8,
    EXTRACT(YEAR FROM _t6.calendar_day) AS year
  FROM _t6 AS _t6
  LEFT JOIN _s7 AS _s7
    ON _s7.calendar_day = _t6.calendar_day
  LEFT JOIN _s13 AS _s13
    ON _s13.calendar_day = _t6.calendar_day
  GROUP BY
    EXTRACT(YEAR FROM _t6.calendar_day)
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
