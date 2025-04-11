WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtxstatus = 'success') AS agg_1,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  (
    100.0 * COALESCE(_s1.agg_1, 0)
  ) / COALESCE(_s1.agg_0, 0) AS success_rate
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.customer_id = sbcustomer.sbcustid
WHERE
  NOT _s1.agg_0 IS NULL AND _s1.agg_0 >= 5
ORDER BY
  success_rate
