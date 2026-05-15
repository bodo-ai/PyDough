WITH _s1 AS (
  SELECT
    sbtxcustid,
    COUNT(*) AS n_rows
  FROM broker.sbtransaction
  WHERE
    CAST(sbtxdatetime AS TIMESTAMP) < CAST('2023-04-02' AS DATE)
    AND CAST(sbtxdatetime AS TIMESTAMP) >= CAST('2023-04-01' AS DATE)
    AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM broker.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  3 DESC NULLS LAST
LIMIT 1
