WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-'[0],
      CAST(YEAR(CAST(sbcustjoindate AS TIMESTAMP))[0] AS VARCHAR),
      CAST(LPAD(MONTH(CAST(sbcustjoindate AS TIMESTAMP)), 2, '0')[1] AS VARCHAR)
    ) AS month,
    COUNT(*) AS n_rows
  FROM postgres.main.sbcustomer
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD('MONTH', -6, CURRENT_TIMESTAMP))
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-'[0],
      CAST(YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))[0] AS VARCHAR),
      CAST(LPAD(MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)), 2, '0')[1] AS VARCHAR)
    ) AS month,
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount
  FROM postgres.main.sbcustomer AS sbcustomer
  JOIN mysql.broker.sbtransaction AS sbtransaction
    ON MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = MONTH(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = YEAR(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', DATE_ADD('MONTH', -6, CURRENT_TIMESTAMP))
  GROUP BY
    1
)
SELECT
  _s2.month,
  _s2.n_rows AS customer_signups,
  _s3.avg_sbtxamount AS avg_tx_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.month = _s3.month
