WITH _s1 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)) AS expr_1,
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATETIME)) AS expr_2,
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    EXTRACT(MONTH FROM CAST(sbtxdatetime AS DATETIME)),
    EXTRACT(YEAR FROM CAST(sbtxdatetime AS DATETIME)),
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.expr_1 = EXTRACT(YEAR FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
  AND _s1.expr_2 = EXTRACT(MONTH FROM CAST(sbcustomer.sbcustjoindate AS DATETIME))
  AND _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  COALESCE(_s1.n_rows, 0) DESC
LIMIT 1
