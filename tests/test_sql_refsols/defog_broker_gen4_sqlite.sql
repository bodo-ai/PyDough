WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    sbtxcustid
  FROM main.sbtransaction
  WHERE
    DATE(sbtxdatetime, 'start of day') = DATE('2023-04-01') AND sbtxtype = 'sell'
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  COALESCE(_s1.n_rows, 0) DESC
LIMIT 1
