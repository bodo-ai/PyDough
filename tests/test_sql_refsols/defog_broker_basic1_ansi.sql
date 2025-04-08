WITH _s2 AS (
  SELECT DISTINCT
    sbcustcountry AS country
  FROM main.sbcustomer
), _s3 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxamount) AS agg_1,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
    AND sbtransaction.sbtxdatetime >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -30, 'DAY'))
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
