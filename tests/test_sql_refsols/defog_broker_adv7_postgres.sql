WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustjoindate AS TIMESTAMP)),
      LPAD(CAST(EXTRACT(MONTH FROM CAST(sbcustjoindate AS TIMESTAMP)) AS TEXT), 2, '0')
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND sbcustjoindate >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP - INTERVAL '6 MONTH')
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)),
      LPAD(
        CAST(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) AS TEXT),
        2,
        '0'
      )
    ) AS month,
    AVG(CAST(sbtransaction.sbtxamount AS DECIMAL)) AS avg_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)
    AND sbcustomer.sbcustjoindate >= DATE_TRUNC('MONTH', CURRENT_TIMESTAMP - INTERVAL '6 MONTH')
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
