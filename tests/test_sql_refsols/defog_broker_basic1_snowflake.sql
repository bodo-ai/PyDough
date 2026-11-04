WITH _s2 AS (
  SELECT DISTINCT
    sbcustcountry
  FROM broker.sbcustomer
), _s1 AS (
  SELECT
    sbtxcustid,
    COUNT(*) AS n_rows,
    SUM(sbtxamount) AS sum_sbtxamount
  FROM broker.sbtransaction
  WHERE
    sbtxdatetime >= DATE_TRUNC(
      'DAY',
      DATEADD(DAY, -30, CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS TIMESTAMPNTZ))
    )
  GROUP BY
    1
), _s3 AS (
  SELECT
    sbcustomer.sbcustcountry,
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_sbtxamount) AS sum_sum_sbtxamount
  FROM broker.sbcustomer AS sbcustomer
  JOIN _s1 AS _s1
    ON _s1.sbtxcustid = sbcustomer.sbcustid
  GROUP BY
    1
)
SELECT
  _s2.sbcustcountry AS country,
  COALESCE(_s3.sum_n_rows, 0) AS num_transactions,
  COALESCE(_s3.sum_sum_sbtxamount, 0) AS total_amount
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.sbcustcountry = _s3.sbcustcountry
