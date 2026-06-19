SELECT
  sbcustcountry AS country,
  100 * (
    COUNT_IF(sbcuststatus = 'active') / COUNT(*)
  ) AS ar
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate <= TO_DATE('2022-12-31', 'YYYY-MM-DD')
  AND sbcustjoindate >= TO_DATE('2022-01-01', 'YYYY-MM-DD')
GROUP BY
  sbcustcountry
