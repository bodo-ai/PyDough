WITH _s1 AS (
  SELECT
    COUNT(*) AS agg_2,
    SUM(sbtxamount) AS agg_5,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    sbtxcustid
), _t0 AS (
  SELECT
    SUM(_s1.agg_2) AS agg_4,
    SUM(_s1.agg_5) AS agg_7,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON _s1.customer_id = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  country,
  COALESCE(agg_4, 0) AS num_transactions,
  COALESCE(agg_7, 0) AS total_amount
FROM _t0
