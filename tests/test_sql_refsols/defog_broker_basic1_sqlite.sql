WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxcustid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    sbtxcustid
), _t0 AS (
  SELECT
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_sbtxamount) AS sum_sum_sbtxamount,
    sbcustomer.sbcustcountry
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON _s1.sbtxcustid = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  sbcustcountry AS country,
  COALESCE(sum_n_rows, 0) AS num_transactions,
  COALESCE(sum_sum_sbtxamount, 0) AS total_amount
FROM _t0
