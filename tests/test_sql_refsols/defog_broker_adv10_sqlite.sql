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
  ON CAST(STRFTIME('%Y', _s1.sbtxdatetime) AS INTEGER) = CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER)
  AND CAST(STRFTIME('%m', _s1.sbtxdatetime) AS INTEGER) = CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)
  AND _s1.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  CAST(STRFTIME('%Y', _s1.sbtxdatetime) AS INTEGER),
  CAST(STRFTIME('%m', _s1.sbtxdatetime) AS INTEGER),
  1
ORDER BY
  3 DESC
LIMIT 1
