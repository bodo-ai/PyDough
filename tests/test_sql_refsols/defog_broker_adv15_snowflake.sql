SELECT
  sbcustcountry AS country,
  100 * (
    COUNT_IF(sbcuststatus = 'active') / COUNT(*)
  ) AS ar
FROM broker.sbcustomer
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  1
