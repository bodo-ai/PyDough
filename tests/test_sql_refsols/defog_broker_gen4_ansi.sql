WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  WHERE
    CAST(sbtxdatetime AS TIMESTAMP) < CAST('2023-04-02' AS DATE)
    AND CAST(sbtxdatetime AS TIMESTAMP) >= CAST('2023-04-01' AS DATE)
    AND sbtxtype = 'sell'
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s1.agg_0, 0) AS num_tx
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
ORDER BY
  num_tx DESC
LIMIT 1
