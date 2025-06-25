SELECT
  sbcustcountry AS country,
  100 * COALESCE(COALESCE(SUM(sbcuststatus = 'active'), 0) / COUNT(*), 0.0) AS ar
FROM main.sbcustomer
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  sbcustcountry
