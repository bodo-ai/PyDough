WITH _t0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtransaction.sbtxamount) AS sum_sbtxamount
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbcustomer AS sbcustomer
    ON LOWER(sbcustomer.sbcustcountry) = 'usa'
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  WHERE
    sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
)
SELECT
  CASE WHEN n_rows > 0 THEN n_rows ELSE NULL END AS n_transactions,
  COALESCE(sum_sbtxamount, 0) AS total_amount
FROM _t0
