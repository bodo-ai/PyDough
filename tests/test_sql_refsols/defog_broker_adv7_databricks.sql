WITH _s2 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustjoindate AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(sbcustjoindate AS TIMESTAMP)), 2, '0')
    ) AS month,
    COUNT(*) AS n_rows
  FROM main.sbcustomer
  WHERE
    sbcustjoindate < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
    AND sbcustjoindate >= TRUNC(ADD_MONTHS(CURRENT_TIMESTAMP(), -6), 'MONTH')
  GROUP BY
    1
), _s3 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)), 2, '0')
    ) AS month,
    AVG(sbtransaction.sbtxamount) AS avg_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = EXTRACT(MONTH FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) = EXTRACT(YEAR FROM CAST(sbtransaction.sbtxdatetime AS TIMESTAMP))
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbcustomer.sbcustjoindate < TRUNC(CURRENT_TIMESTAMP(), 'MONTH')
    AND sbcustomer.sbcustjoindate >= TRUNC(ADD_MONTHS(CURRENT_TIMESTAMP(), -6), 'MONTH')
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
