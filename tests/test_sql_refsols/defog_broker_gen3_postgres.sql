WITH _s1 AS (
  SELECT
    MIN(sbtxdatetime) AS min_sbtxdatetime,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    2
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  CAST(CAST(EXTRACT(EPOCH FROM CAST(_s1.min_sbtxdatetime AS TIMESTAMP) - CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)) AS BIGINT) AS DOUBLE PRECISION) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
