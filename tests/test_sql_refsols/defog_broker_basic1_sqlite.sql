WITH _s1 AS (
  SELECT
    sbtxcustid,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    1
)
SELECT
  sbcustomer.sbcustcountry AS country,
  SUM(_s1.n_rows) AS num_transactions,
  COALESCE(SUM(_s1.sum_sbtxamount), 0) AS total_amount
FROM main.sbcustomer AS sbcustomer
JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1
