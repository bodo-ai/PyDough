SELECT
  sbcustcountry AS country,
  100 * (
    COALESCE(SUM(sbcuststatus = 'active'), 0) / COUNT(*)
  ) AS ar
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate <= TO_DATE('2022-12-31', 'YYYY-MM-DD')
  AND sbcustjoindate >= TO_DATE('2022-01-01', 'YYYY-MM-DD')
GROUP BY
  sbcustcountry
