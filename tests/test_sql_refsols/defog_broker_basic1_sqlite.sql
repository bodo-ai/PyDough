WITH _t2 AS (
  SELECT DISTINCT
    sbcustcountry AS country
  FROM main.sbcustomer
), _t3_2 AS (
  SELECT
    COUNT() AS agg_0,
    SUM(sbtransaction.sbtxamount) AS agg_1,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
    AND sbtransaction.sbtxdatetime >= DATE(DATETIME('now', '-30 day'), 'start of day')
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  _t2.country,
  COALESCE(_t3.agg_0, 0) AS num_transactions,
  COALESCE(_t3.agg_1, 0) AS total_amount
FROM _t2 AS _t2
LEFT JOIN _t3_2 AS _t3
  ON _t2.country = _t3.country
