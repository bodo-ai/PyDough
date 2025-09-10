WITH _s1 AS (
  SELECT
    sbtxcustid,
    MIN(sbtxdatetime) AS min_sbtxdatetime
  FROM main.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  CAST(EXTRACT(EPOCH FROM (
    CAST(_s1.min_sbtxdatetime AS TIMESTAMP) - CAST(sbcustomer.sbcustjoindate AS TIMESTAMP)
  )) AS DOUBLE PRECISION) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
