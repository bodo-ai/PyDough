WITH _t0 AS (
  SELECT
    COUNT(*) AS agg_0,
    SUM(_s0.sbtxamount) AS agg_1
  FROM main.sbtransaction AS _s0
  JOIN main.sbcustomer AS _s1
    ON LOWER(_s1.sbcustcountry) = 'usa' AND _s0.sbtxcustid = _s1.sbcustid
  WHERE
    _s0.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND _s0.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
)
SELECT
  CASE WHEN _t0.agg_0 > 0 THEN _t0.agg_0 ELSE NULL END AS n_transactions,
  COALESCE(_t0.agg_1, 0) AS total_amount
FROM _t0 AS _t0
