SELECT
  sbcustcountry AS country,
  100 * (
    COALESCE(SUM(sbcuststatus = 'active'), 0) / COUNT(*)
  ) AS ar
FROM broker.sbCustomer
WHERE
  sbcustjoindate <= CAST('2022-12-31' AS DATE)
  AND sbcustjoindate >= CAST('2022-01-01' AS DATE)
GROUP BY
  1
