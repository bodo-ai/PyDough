SELECT
  sbcustcountry AS country,
  100 * (
    COALESCE(SUM(sbcuststatus = 'active'), 0) / COUNT(*)
  ) AS ar
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  sbcustcountry
