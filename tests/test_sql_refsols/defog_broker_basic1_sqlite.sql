WITH _s3 AS (
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
    SUM(_s3.agg_2) AS agg_4,
    SUM(_s3.agg_5) AS agg_7,
    _s0.sbcustcountry AS country
  FROM main.sbcustomer AS _s0
  LEFT JOIN _s3 AS _s3
    ON _s0.sbcustid = _s3.customer_id
  GROUP BY
    _s0.sbcustcountry
)
SELECT
  country,
  COALESCE(agg_4, 0) AS num_transactions,
  COALESCE(agg_7, 0) AS total_amount
FROM _t0
