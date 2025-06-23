WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_2,
    sbtxcustid AS customer_id,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    sbtxcustid
), _t0 AS (
  SELECT
    sbcustomer.sbcustcountry AS country,
    SUM(_s1.agg_2) AS sum_agg_2,
    SUM(_s1.sum_sbtxamount) AS sum_sum_sbtxamount
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_id = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  country,
  COALESCE(sum_agg_2, 0) AS num_transactions,
  COALESCE(sum_sum_sbtxamount, 0) AS total_amount
FROM _t0
