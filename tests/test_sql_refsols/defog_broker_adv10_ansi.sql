WITH _s3 AS (
  SELECT
    sbcustomer.sbcustid AS _id,
    COUNT() AS agg_0
  FROM main.sbcustomer AS sbcustomer
  JOIN main.sbtransaction AS sbtransaction
    ON EXTRACT(MONTH FROM sbcustomer.sbcustjoindate) = EXTRACT(MONTH FROM sbtransaction.sbtxdatetime)
    AND EXTRACT(YEAR FROM sbcustomer.sbcustjoindate) = EXTRACT(YEAR FROM sbtransaction.sbtxdatetime)
    AND sbcustomer.sbcustid = sbtransaction.sbtxcustid
  GROUP BY
    sbcustomer.sbcustid
)
SELECT
  sbcustomer.sbcustid AS _id,
  sbcustomer.sbcustname AS name,
  COALESCE(_s3.agg_0, 0) AS num_transactions
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s3 AS _s3
  ON _s3._id = sbcustomer.sbcustid
ORDER BY
  num_transactions DESC
LIMIT 1
