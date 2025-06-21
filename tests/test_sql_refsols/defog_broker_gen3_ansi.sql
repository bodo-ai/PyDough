WITH _s1 AS (
  SELECT
    MIN(sbtxdatetime) AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS cust_id,
  DATEDIFF(CAST(_s1.agg_0 AS DATETIME), CAST(sbcustomer.sbcustjoindate AS DATETIME), SECOND) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
