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
  (
    (
      (
        DATEDIFF(DAY, CAST(sbcustomer.sbcustjoindate AS DATE), CAST(_s1.min_sbtxdatetime AS DATE)) * 24 + EXTRACT(HOUR FROM CAST(_s1.min_sbtxdatetime AS TIMESTAMP)) - EXTRACT(HOUR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
      ) * 60 + EXTRACT(MINUTE FROM CAST(_s1.min_sbtxdatetime AS TIMESTAMP)) - EXTRACT(MINUTE FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
    ) * 60 + EXTRACT(SECOND FROM CAST(_s1.min_sbtxdatetime AS TIMESTAMP)) - EXTRACT(SECOND FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  ) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
