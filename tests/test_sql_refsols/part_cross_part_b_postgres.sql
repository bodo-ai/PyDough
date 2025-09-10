WITH _s0 AS (
  SELECT DISTINCT
    sbcuststate
  FROM main.sbcustomer
), _t2 AS (
  SELECT
    sbtxdatetime
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) = 2023
), _s1 AS (
  SELECT DISTINCT
    DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month
  FROM _t2
), _s3 AS (
  SELECT DISTINCT
    DATE_TRUNC('MONTH', CAST(sbtxdatetime AS TIMESTAMP)) AS month
  FROM _t2
), _s9 AS (
  SELECT
    _s3.month,
    _s2.sbcuststate,
    COUNT(*) AS n_rows
  FROM _s0 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP)) = 2023
    AND _s3.month = DATE_TRUNC('MONTH', CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
  JOIN main.sbcustomer AS sbcustomer
    ON _s2.sbcuststate = sbcustomer.sbcuststate
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    1,
    2
)
SELECT
  _s0.sbcuststate AS state,
  _s1.month AS month_of_year,
  SUM(COALESCE(_s9.n_rows, 0)) OVER (PARTITION BY _s0.sbcuststate ORDER BY _s1.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.sbcuststate = _s9.sbcuststate AND _s1.month = _s9.month
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
