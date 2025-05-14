WITH _s0 AS (
  SELECT
    COUNT(sbtxcustid) AS transaction_count,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
), _t0 AS (
  SELECT
    SUM(_s0.transaction_count) AS transaction_count
  FROM _s0 AS _s0
  JOIN main.sbcustomer AS sbcustomer
    ON _s0.customer_id = sbcustomer.sbcustid
    AND sbcustomer.sbcustjoindate >= DATE_ADD(CURRENT_TIMESTAMP(), -70, 'DAY')
)
SELECT
  COALESCE(transaction_count, 0) AS transaction_count
FROM _t0
