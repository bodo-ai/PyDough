WITH _s1 AS (
  SELECT
    sbtxcustid,
    sbtxdatetime
  FROM main.sbtransaction
)
SELECT
  _s1.sbtxcustid AS _id,
  MAX(sbcustomer.sbcustname) AS name,
  COUNT(*) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON EXTRACT(MONTH FROM CAST(_s1.sbtxdatetime AS TIMESTAMP)) = EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND EXTRACT(YEAR FROM CAST(_s1.sbtxdatetime AS TIMESTAMP)) = EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  EXTRACT(MONTH FROM CAST(_s1.sbtxdatetime AS TIMESTAMP)),
  EXTRACT(YEAR FROM CAST(_s1.sbtxdatetime AS TIMESTAMP)),
  1
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
