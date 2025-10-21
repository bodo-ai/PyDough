WITH _s1 AS (
  SELECT
    MONTH(CAST(sbtxdatetime AS TIMESTAMP)) AS month_sbtxdatetime,
    YEAR(CAST(sbtxdatetime AS TIMESTAMP)) AS year_sbtxdatetime,
    sbtxcustid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  GROUP BY
    1,
    2,
    3
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.month_sbtxdatetime = MONTH(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
  AND _s1.year_sbtxdatetime = YEAR(CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
