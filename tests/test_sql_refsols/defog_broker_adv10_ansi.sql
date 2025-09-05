WITH _s1 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATETIME)) AS month_sbtxdatetime,
    COUNT(*) AS n_rows,
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) AS year_sbtxdatetime,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    1,
    3,
    4
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.month_sbtxdatetime = EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
  AND _s1.year_sbtxdatetime = EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
ORDER BY
  3 DESC
LIMIT 1
