WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount,
    sbtxcustid
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= CAST(DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL '-30' DAY) AS DATE)
  GROUP BY
    3
)
SELECT
  sbcustomer.sbcustcountry AS country,
  COALESCE(SUM(_s1.n_rows), 0) AS num_transactions,
  COALESCE(SUM(_s1.sum_sbtxamount), 0) AS total_amount
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
GROUP BY
  1
