WITH _s0 AS (
  SELECT
    ANY_VALUE(pr_release) AS release_date
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _t6 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s1 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM calendar_day) AS year
  FROM _t6
), _t9 AS (
  SELECT
    pr_id AS _id,
    pr_name AS name
  FROM main.products
), _s7 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _t8.calendar_day) AS year
  FROM _t6 AS _t8
  JOIN main.devices AS devices
    ON _t8.calendar_day = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN _t9 AS _t9
    ON _t9._id = devices.de_product_id AND _t9.name = 'GoldCopper-Star'
  GROUP BY
    EXTRACT(YEAR FROM _t8.calendar_day)
), _s15 AS (
  SELECT
    COUNT() AS agg_2,
    EXTRACT(YEAR FROM _t11.calendar_day) AS year
  FROM _t6 AS _t11
  JOIN main.incidents AS incidents
    ON _t11.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN _t9 AS _t12
    ON _t12._id = devices.de_product_id AND _t12.name = 'GoldCopper-Star'
  GROUP BY
    EXTRACT(YEAR FROM _t11.calendar_day)
), _t0 AS (
  SELECT
    COALESCE(_s7.agg_1, 0) AS bought,
    ROUND(
      SUM(COALESCE(_s15.agg_2, 0)) OVER (ORDER BY _s1.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s7.agg_1, 0)) OVER (ORDER BY _s1.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(_s15.agg_2, 0) AS incidents,
    ROUND(
      (
        100.0 * (
          COALESCE(_s7.agg_1, 0) - LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s1.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s7.agg_1, 0), 1) OVER (ORDER BY _s1.year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(_s15.agg_2, 0) - LAG(COALESCE(_s15.agg_2, 0), 1) OVER (ORDER BY _s1.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s15.agg_2, 0), 1) OVER (ORDER BY _s1.year NULLS LAST),
      2
    ) AS pct_incident_change,
    _s1.year - EXTRACT(YEAR FROM _s0.release_date) AS years_since_release
  FROM _s0 AS _s0
  CROSS JOIN _s1 AS _s1
  LEFT JOIN _s7 AS _s7
    ON _s1.year = _s7.year
  LEFT JOIN _s15 AS _s15
    ON _s1.year = _s15.year
  WHERE
    _s1.year >= EXTRACT(YEAR FROM _s0.release_date)
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
