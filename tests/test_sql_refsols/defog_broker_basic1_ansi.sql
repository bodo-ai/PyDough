WITH _s1 AS (
  SELECT
    sbtxcustid AS customer_id,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY'))
  GROUP BY
    sbtxcustid
), _t0 AS (
  SELECT
    sbcustomer.sbcustcountry AS country,
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_sbtxamount) AS sum_sum_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_id = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  country,
  COALESCE(sum_n_rows, 0) AS num_transactions,
  COALESCE(sum_sum_sbtxamount, 0) AS total_amount
FROM _t0
