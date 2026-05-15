SELECT
  sbcustcountry AS country,
  100 * (
    CAST(COALESCE(SUM(CASE WHEN sbcuststatus = 'active' THEN 1 ELSE 0 END), 0) AS DOUBLE PRECISION) / COUNT(*)
  ) AS ar
FROM main.sbcustomer
WHERE
  sbcustjoindate <= CAST('2022-12-31' AS DATE)
  AND sbcustjoindate >= CAST('2022-01-01' AS DATE)
GROUP BY
  1
