WITH _s1 AS (
  SELECT
    sbtxcustid,
    COUNT(*) AS n_rows
  FROM main.sbtransaction
  WHERE
    DATE(sbtxdatetime, 'start of day') = DATE('2023-04-01') AND sbtxtype = 'sell'
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  _s1.n_rows AS num_tx
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  3 DESC
LIMIT 1
