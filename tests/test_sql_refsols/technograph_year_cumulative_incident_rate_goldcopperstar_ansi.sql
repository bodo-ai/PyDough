WITH _t5 AS (
  SELECT
    ca_dt AS calendar_day
  FROM main.calendar
), _s4 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM calendar_day) AS year
  FROM _t5
), _s5 AS (
  SELECT
    ANY_VALUE(products.pr_release) AS agg_2,
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM _t7.calendar_day) AS year
  FROM _t5 AS _t7
  JOIN main.devices AS devices
    ON _t7.calendar_day = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'GoldCopper-Star'
  GROUP BY
    EXTRACT(YEAR FROM _t7.calendar_day)
), _s13 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _t10.calendar_day) AS year
  FROM _t5 AS _t10
  JOIN main.incidents AS incidents
    ON _t10.calendar_day = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'GoldCopper-Star'
  GROUP BY
    EXTRACT(YEAR FROM _t10.calendar_day)
), _t0 AS (
  SELECT
    COALESCE(_s5.agg_0, 0) AS bought,
    COALESCE(_s13.agg_1, 0) AS incidents,
    ROUND(
      (
        100.0 * (
          COALESCE(_s5.agg_0, 0) - LAG(COALESCE(_s5.agg_0, 0), 1) OVER (ORDER BY _s4.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s5.agg_0, 0), 1) OVER (ORDER BY _s4.year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(_s13.agg_1, 0) - LAG(COALESCE(_s13.agg_1, 0), 1) OVER (ORDER BY _s4.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s13.agg_1, 0), 1) OVER (ORDER BY _s4.year NULLS LAST),
      2
    ) AS pct_incident_change,
    ROUND(
      SUM(COALESCE(_s13.agg_1, 0)) OVER (ORDER BY _s4.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s5.agg_0, 0)) OVER (ORDER BY _s4.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    _s4.year - EXTRACT(YEAR FROM _s5.agg_2) AS years_since_release
  FROM _s4 AS _s4
  LEFT JOIN _s5 AS _s5
    ON _s4.year = _s5.year
  LEFT JOIN _s13 AS _s13
    ON _s13.year = _s4.year
  WHERE
    _s4.year >= EXTRACT(YEAR FROM _s5.agg_2)
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
