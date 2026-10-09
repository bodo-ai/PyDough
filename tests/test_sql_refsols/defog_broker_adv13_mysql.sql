SELECT
  sbCustCountry AS cust_country,
  COUNT(*) AS TAC
FROM broker.sbCustomer
WHERE
  sbCustJoinDate >= CAST('2023-01-01' AS DATE)
GROUP BY
  1
