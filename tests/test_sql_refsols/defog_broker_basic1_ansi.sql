WITH _s2 AS (
  SELECT DISTINCT
    sbcustcountry AS country
  FROM main.sbcustomer
), _s1 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtxamount) AS agg_1,
    sbtxcustid AS customer_id
  FROM main.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY'))
  GROUP BY
    sbtxcustid
), _s3 AS (
  SELECT
    SUM(_s1.agg_0) AS agg_0,
    SUM(_s1.agg_1) AS agg_1,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  JOIN _s1 AS _s1
    ON _s1.customer_id = sbcustomer.sbcustid
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  _s2.country,
  COALESCE(_s3.agg_0, 0) AS num_transactions,
  COALESCE(_s3.agg_1, 0) AS total_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.country = _s3.country
