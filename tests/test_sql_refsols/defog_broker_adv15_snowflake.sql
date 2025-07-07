SELECT
  sbcustcountry AS country,
  100 * COALESCE(COALESCE(COUNT_IF(sbcuststatus = 'active'), 0) / COUNT(*), 0.0) AS ar
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  sbcustcountry
