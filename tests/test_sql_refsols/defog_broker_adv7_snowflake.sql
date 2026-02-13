WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      YEAR(CAST(sbcustjoindate AS TIMESTAMP)),
      LPAD(MONTH(CAST(sbcustjoindate AS TIMESTAMP)), 2, '0')
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    AND sbcustjoindate >= DATE_TRUNC(
      'MONTH',
      DATEADD(MONTH, -6, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    )
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)),
      LPAD(MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)), 2, '0')
    ) AS month,
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = MONTH(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = YEAR(CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC(
      'MONTH',
      DATEADD(MONTH, -6, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    )
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
