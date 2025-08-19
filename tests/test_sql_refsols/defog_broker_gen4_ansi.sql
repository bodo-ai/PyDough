WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  WHERE
    CAST(sbtxdatetime AS TIMESTAMP) < CAST('2023-04-02' AS DATE)
    AND CAST(sbtxdatetime AS TIMESTAMP) >= CAST('2023-04-01' AS DATE)
    AND sbtxtype = 'sell'
  GROUP BY
    2
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  3 DESC
LIMIT 1
