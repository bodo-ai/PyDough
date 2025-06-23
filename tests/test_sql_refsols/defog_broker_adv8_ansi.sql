SELECT
  CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE NULL END AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
JOIN main.sbcustomer AS sbcustomer
  ON LOWER(sbcustomer.sbcustcountry) = 'usa'
  AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
WHERE
  sbtransaction.sbtxdatetime < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
  AND sbtransaction.sbtxdatetime >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -1, 'WEEK')
