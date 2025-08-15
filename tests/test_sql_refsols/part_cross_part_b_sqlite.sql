WITH _s0 AS (
  SELECT DISTINCT
    sbcuststate
  FROM main.sbcustomer
), _t2 AS (
  SELECT
    sbtxdatetime
  FROM main.sbtransaction
  WHERE
    CAST(STRFTIME('%Y', sbtxdatetime) AS INTEGER) = 2023
), _s1 AS (
  SELECT DISTINCT
    DATE(sbtxdatetime, 'start of month') AS month
  FROM _t2
), _s3 AS (
  SELECT DISTINCT
    DATE(sbtxdatetime, 'start of month') AS month
  FROM _t2
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s3.month,
    _s2.sbcuststate
  FROM _s0 AS _s2
  LEFT JOIN _s3 AS _s3
    ON TRUE
  JOIN main.sbtransaction AS sbtransaction
    ON CAST(STRFTIME('%Y', sbtransaction.sbtxdatetime) AS INTEGER) = 2023
    AND _s3.month = DATE(sbtransaction.sbtxdatetime, 'start of month')
  JOIN main.sbcustomer AS sbcustomer
    ON _s2.sbcuststate = sbcustomer.sbcuststate
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    _s3.month,
    _s2.sbcuststate
)
SELECT
  _s0.sbcuststate AS state,
  _s1.month AS month_of_year,
  SUM(COALESCE(_s9.n_rows, 0)) OVER (PARTITION BY _s0.sbcuststate ORDER BY _s1.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON TRUE
LEFT JOIN _s9 AS _s9
  ON _s0.sbcuststate = _s9.sbcuststate AND _s1.month = _s9.month
ORDER BY
  _s0.sbcuststate,
  _s1.month
