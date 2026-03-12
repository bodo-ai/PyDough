SELECT
  sbcustcountry AS country,
  100 * (
    COUNT_IF(sbcuststatus = 'active') / COUNT(*)
  ) AS ar
FROM broker.sbcustomer
WHERE
  sbcustjoindate <= CAST('2022-12-31' AS DATE)
  AND sbcustjoindate >= CAST('2022-01-01' AS DATE)
GROUP BY
  1
