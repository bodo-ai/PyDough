WITH _s1 AS (
  SELECT
    MIN(sbtxdatetime) AS min_sbTxDateTime,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    2
)
SELECT
  sbCustomer.sbcustid AS cust_id,
  TIMESTAMPDIFF(SECOND, sbCustomer.sbcustjoindate, _s1.min_sbTxDateTime) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM main.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
