WITH _s1 AS (
  SELECT
    sbtxcustid,
    sbtxdatetime
  FROM main.sbtransaction
)
SELECT
  _s1.sbtxcustid AS _id,
  ANY_VALUE(sbcustomer.sbcustname) AS name,
  COUNT(*) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON MONTH(CAST(_s1.sbtxdatetime AS TIMESTAMP)) = MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND YEAR(CAST(_s1.sbtxdatetime AS TIMESTAMP)) = YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1,
  MONTH(CAST(_s1.sbtxdatetime AS TIMESTAMP)),
  YEAR(CAST(_s1.sbtxdatetime AS TIMESTAMP))
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
