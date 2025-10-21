WITH _s1 AS (
  SELECT
    sbtxcustid,
    sbtxdatetime
  FROM main.sbtransaction
), _t0 AS (
  SELECT
    _s1.sbtxcustid,
    ANY_VALUE(sbcustomer.sbcustname) AS anything_sbcustname,
    COUNT(*) AS n_rows
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON MONTH(CAST(_s1.sbtxdatetime AS TIMESTAMP)) = MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
    AND YEAR(CAST(_s1.sbtxdatetime AS TIMESTAMP)) = YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
    AND _s1.sbtxcustid = sbcustomer.sbcustid
  GROUP BY
    1,
    MONTH(CAST(_s1.sbtxdatetime AS TIMESTAMP)),
    YEAR(CAST(_s1.sbtxdatetime AS TIMESTAMP))
)
SELECT
  sbtxcustid AS _id,
  anything_sbcustname AS name,
  n_rows * IFF(NOT sbtxcustid IS NULL, 1, 0) AS num_transactions
FROM _t0
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
