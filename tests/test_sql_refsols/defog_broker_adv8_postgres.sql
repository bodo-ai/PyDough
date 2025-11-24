WITH _u_0 AS (
  SELECT
    sbcustid AS _u_1
  FROM main.sbcustomer
  WHERE
    LOWER(sbcustcountry) = 'usa'
  GROUP BY
    1
)
SELECT
  NULLIF(COUNT(*), 0) AS n_transactions,
  COALESCE(SUM(sbtransaction.sbtxamount), 0) AS total_amount
FROM main.sbtransaction AS sbtransaction
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbtransaction.sbtxcustid
WHERE
  NOT _u_0._u_1 IS NULL
  AND sbtransaction.sbtxdatetime < DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  )
  AND sbtransaction.sbtxdatetime >= DATE_TRUNC(
    'DAY',
    CURRENT_TIMESTAMP - CAST((
      EXTRACT(DOW FROM CURRENT_TIMESTAMP) + 6
    ) % 7 || ' days' AS INTERVAL)
  ) - INTERVAL '1 WEEK'
