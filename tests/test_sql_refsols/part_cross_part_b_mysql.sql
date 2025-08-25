WITH _s0 AS (
  SELECT DISTINCT
    sbcuststate
  FROM main.sbcustomer
), _t2 AS (
  SELECT
    sbtxdatetime
  FROM main.sbtransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
), _s1 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(YEAR(CAST(sbtxdatetime AS DATETIME)), ' ', MONTH(CAST(sbtxdatetime AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS month
  FROM _t2
), _s3 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(YEAR(CAST(sbtxdatetime AS DATETIME)), ' ', MONTH(CAST(sbtxdatetime AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS month
  FROM _t2
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s3.month,
    _s2.sbcuststate
  FROM _s0 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS DATETIME)) = 2023
    AND _s3.month = STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sbtransaction.sbtxdatetime AS DATETIME)),
        ' ',
        MONTH(CAST(sbtransaction.sbtxdatetime AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    )
  JOIN main.sbcustomer AS sbcustomer
    ON _s2.sbcuststate = sbcustomer.sbcuststate
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    2,
    3
)
SELECT
  _s0.sbcuststate COLLATE utf8mb4_bin AS state,
  _s1.month AS month_of_year,
  SUM(COALESCE(_s9.n_rows, 0)) OVER (PARTITION BY _s0.sbcuststate ORDER BY _s1.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.sbcuststate = _s9.sbcuststate AND _s1.month = _s9.month
ORDER BY
  1,
  2
