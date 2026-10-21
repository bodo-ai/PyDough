WITH _s2 AS (
  SELECT
    ca_dt
  FROM main.calendar
), _s3 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s0.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s0
  JOIN main.devices AS devices
    ON _s0.ca_dt = DATE_TRUNC('DAY', CAST(devices.de_purchase_ts AS TIMESTAMP))
  GROUP BY
    1
), _s6 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s2.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    ANY_VALUE(_s3.n_rows) AS anything_n_rows
  FROM _s2 AS _s2
  JOIN _s3 AS _s3
    ON _s3.year_ca_dt = EXTRACT(YEAR FROM CAST(_s2.ca_dt AS TIMESTAMP))
  GROUP BY
    1
), _s7 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s4.ca_dt AS TIMESTAMP)) AS year_ca_dt,
    COUNT(*) AS n_rows
  FROM _s2 AS _s4
  JOIN main.incidents AS incidents
    ON _s4.ca_dt = DATE_TRUNC('DAY', CAST(incidents.in_error_report_ts AS TIMESTAMP))
  GROUP BY
    1
)
SELECT
  _s6.year_ca_dt AS yr,
  ROUND(
    SUM(COALESCE(_s7.n_rows, 0)) OVER (
      ORDER BY _s6.year_ca_dt NULLS LAST
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) / SUM(COALESCE(_s6.anything_n_rows, 0)) OVER (
      ORDER BY _s6.year_ca_dt NULLS LAST
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    2
  ) AS cum_ir,
  ROUND(
    (
      100.0 * (
        COALESCE(_s6.anything_n_rows, 0) - LAG(COALESCE(_s6.anything_n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt NULLS LAST)
      )
    ) / LAG(COALESCE(_s6.anything_n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt NULLS LAST),
    2
  ) AS pct_bought_change,
  ROUND(
    (
      100.0 * (
        COALESCE(_s7.n_rows, 0) - LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt NULLS LAST)
      )
    ) / LAG(COALESCE(_s7.n_rows, 0), 1) OVER (ORDER BY _s6.year_ca_dt NULLS LAST),
    2
  ) AS pct_incident_change,
  COALESCE(_s6.anything_n_rows, 0) AS bought,
  COALESCE(_s7.n_rows, 0) AS incidents
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year_ca_dt = _s7.year_ca_dt
ORDER BY
  1
