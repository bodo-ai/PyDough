WITH _s1 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS TIMESTAMP)) AS expr_1,
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS TIMESTAMP)) AS expr_2,
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    1,
    2,
    4
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.expr_1 = EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND _s1.expr_2 = EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS TIMESTAMP))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
