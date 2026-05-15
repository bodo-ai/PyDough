SELECT
  sbcustcountry AS cust_country,
  COUNT(*) AS TAC
FROM MAIN.SBCUSTOMER
WHERE
  sbcustjoindate >= TO_DATE('2023-01-01', 'YYYY-MM-DD')
GROUP BY
  sbcustcountry
