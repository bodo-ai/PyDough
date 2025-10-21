WITH _s1 AS (
  SELECT
    sbtxcustid,
    sbtxdatetime
  FROM main.sbtransaction
), _t0 AS (
  SELECT
    _s1.sbtxcustid,
    MAX(sbcustomer.sbcustname) AS anything_sbcustname,
    COUNT(*) AS n_rows
  FROM main.sbcustomer AS sbcustomer
  LEFT JOIN _s1 AS _s1
    ON CAST(STRFTIME('%Y', _s1.sbtxdatetime) AS INTEGER) = CAST(STRFTIME('%Y', sbcustomer.sbcustjoindate) AS INTEGER)
    AND CAST(STRFTIME('%m', _s1.sbtxdatetime) AS INTEGER) = CAST(STRFTIME('%m', sbcustomer.sbcustjoindate) AS INTEGER)
    AND _s1.sbtxcustid = sbcustomer.sbcustid
  GROUP BY
    CAST(STRFTIME('%Y', _s1.sbtxdatetime) AS INTEGER),
    CAST(STRFTIME('%m', _s1.sbtxdatetime) AS INTEGER),
    1
)
SELECT
  sbtxcustid AS _id,
  anything_sbcustname AS name,
  n_rows * IIF(NOT sbtxcustid IS NULL, 1, 0) AS num_transactions
FROM _t0
ORDER BY
  3 DESC
LIMIT 1
