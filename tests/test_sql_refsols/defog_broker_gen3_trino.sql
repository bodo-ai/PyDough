WITH _s1 AS (
  SELECT
    sbtxcustid,
    MIN(sbtxdatetime) AS min_sbtxdatetime
  FROM mysql.broker.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  CAST(DATE_DIFF(
    'SECOND',
    CAST(sbcustomer.sbcustjoindate AS TIMESTAMP),
    CAST(_s1.min_sbtxdatetime AS TIMESTAMP)
  ) AS DOUBLE) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM postgres.main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
