WITH _s23 AS (
  SELECT
    ANY_VALUE(pr_release) AS release_date
  FROM main.products
  WHERE
    pr_name = 'GoldCopper-Star'
), _s13 AS (
  SELECT
    COUNT(*) AS agg_3,
    _s2.ca_dt AS calendar_day
  FROM main.calendar AS _s2
  JOIN main.incidents AS _s3
    ON _s2.ca_dt = DATE_TRUNC('DAY', CAST(_s3.in_error_report_ts AS TIMESTAMP))
  JOIN main.devices AS _s6
    ON _s3.in_device_id = _s6.de_id
  JOIN main.products AS _s9
    ON _s6.de_product_id = _s9.pr_id AND _s9.pr_name = 'GoldCopper-Star'
  GROUP BY
    _s2.ca_dt
), _s22 AS (
  SELECT
    COUNT(*) AS agg_6,
    _s14.ca_dt AS calendar_day
  FROM main.calendar AS _s14
  JOIN main.devices AS _s15
    ON _s14.ca_dt = DATE_TRUNC('DAY', CAST(_s15.de_purchase_ts AS TIMESTAMP))
  JOIN main.products AS _s18
    ON _s15.de_product_id = _s18.pr_id AND _s18.pr_name = 'GoldCopper-Star'
  GROUP BY
    _s14.ca_dt
), _s24 AS (
  SELECT
    SUM(_s13.agg_3) AS agg_5,
    SUM(_s22.agg_6) AS agg_8,
    EXTRACT(YEAR FROM _s1.ca_dt) AS year
  FROM main.calendar AS _s1
  LEFT JOIN _s13 AS _s13
    ON _s1.ca_dt = _s13.calendar_day
  LEFT JOIN _s22 AS _s22
    ON _s1.ca_dt = _s22.calendar_day
  GROUP BY
    EXTRACT(YEAR FROM _s1.ca_dt)
), _t0 AS (
  SELECT
    COALESCE(_s24.agg_8, 0) AS bought,
    ROUND(
      SUM(COALESCE(_s24.agg_5, 0)) OVER (ORDER BY _s24.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(COALESCE(_s24.agg_8, 0)) OVER (ORDER BY _s24.year NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      2
    ) AS cum_ir,
    COALESCE(_s24.agg_5, 0) AS incidents,
    ROUND(
      (
        100.0 * (
          COALESCE(_s24.agg_8, 0) - LAG(COALESCE(_s24.agg_8, 0), 1) OVER (ORDER BY _s24.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s24.agg_8, 0), 1) OVER (ORDER BY _s24.year NULLS LAST),
      2
    ) AS pct_bought_change,
    ROUND(
      (
        100.0 * (
          COALESCE(_s24.agg_5, 0) - LAG(COALESCE(_s24.agg_5, 0), 1) OVER (ORDER BY _s24.year NULLS LAST)
        )
      ) / LAG(COALESCE(_s24.agg_5, 0), 1) OVER (ORDER BY _s24.year NULLS LAST),
      2
    ) AS pct_incident_change,
    _s24.year - EXTRACT(YEAR FROM _s23.release_date) AS years_since_release
  FROM _s23 AS _s23
  CROSS JOIN _s24 AS _s24
  WHERE
    _s24.year >= EXTRACT(YEAR FROM _s23.release_date)
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
