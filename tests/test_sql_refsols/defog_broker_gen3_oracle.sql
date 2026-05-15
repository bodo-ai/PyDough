WITH "_S1" AS (
  SELECT
    sbtxcustid AS SBTXCUSTID,
    MIN(sbtxdatetime) AS MIN_SBTXDATETIME
  FROM MAIN.SBTRANSACTION
  GROUP BY
    sbtxcustid
)
SELECT
  SBCUSTOMER.sbcustid AS cust_id,
  (
    (
      CAST("_S1".MIN_SBTXDATETIME AS DATE) - CAST(SBCUSTOMER.sbcustjoindate AS DATE)
    ) * 86400
  ) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM MAIN.SBCUSTOMER SBCUSTOMER
JOIN "_S1" "_S1"
  ON SBCUSTOMER.sbcustid = "_S1".SBTXCUSTID
