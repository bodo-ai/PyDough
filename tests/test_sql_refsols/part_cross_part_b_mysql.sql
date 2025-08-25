WITH _s0 AS (
  SELECT DISTINCT
    sbcuststate AS sbCustState
  FROM main.sbCustomer
), _t2 AS (
  SELECT
    sbtxdatetime AS sbTxDateTime
  FROM main.sbTransaction
  WHERE
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) = 2023
), _s1 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(YEAR(CAST(sbTxDateTime AS DATETIME)), ' ', MONTH(CAST(sbTxDateTime AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS month
  FROM _t2
), _s3 AS (
  SELECT DISTINCT
    STR_TO_DATE(
      CONCAT(YEAR(CAST(sbTxDateTime AS DATETIME)), ' ', MONTH(CAST(sbTxDateTime AS DATETIME)), ' 1'),
      '%Y %c %e'
    ) AS month
  FROM _t2
), _s9 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s3.month,
    _s2.sbCustState
  FROM _s0 AS _s2
  CROSS JOIN _s3 AS _s3
  JOIN main.sbTransaction AS sbTransaction
    ON EXTRACT(YEAR FROM CAST(sbTransaction.sbtxdatetime AS DATETIME)) = 2023
    AND _s3.month = STR_TO_DATE(
      CONCAT(
        YEAR(CAST(sbTransaction.sbtxdatetime AS DATETIME)),
        ' ',
        MONTH(CAST(sbTransaction.sbtxdatetime AS DATETIME)),
        ' 1'
      ),
      '%Y %c %e'
    )
  JOIN main.sbCustomer AS sbCustomer
    ON _s2.sbCustState = sbCustomer.sbcuststate
    AND sbCustomer.sbcustid = sbTransaction.sbtxcustid
  GROUP BY
    2,
    3
)
SELECT
  _s0.sbCustState COLLATE utf8mb4_bin AS state,
  _s1.month AS month_of_year,
  SUM(COALESCE(_s9.n_rows, 0)) OVER (PARTITION BY _s0.sbCustState ORDER BY _s1.month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
LEFT JOIN _s9 AS _s9
  ON _s0.sbCustState = _s9.sbCustState AND _s1.month = _s9.month
ORDER BY
  1,
  2
