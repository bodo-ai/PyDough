WITH _s1 AS (
  SELECT
    sbtxcustid,
    MIN(sbtxdatetime) AS min_sbtxdatetime
  FROM broker.sbtransaction
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  DATEDIFF(
    SECOND,
    CAST(sbcustomer.sbcustjoindate AS DATETIME),
    CAST(_s1.min_sbtxdatetime AS DATETIME)
  ) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM broker.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
