WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(sbtransaction.sbtxamount) AS agg_1
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbcustomer AS sbcustomer
    ON LOWER(sbcustomer.sbcustcountry) = 'usa'
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0 AS _t0
