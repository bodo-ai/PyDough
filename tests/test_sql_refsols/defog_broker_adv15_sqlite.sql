SELECT
  sbcustcountry AS country,
  100 * (
    CAST(COALESCE(SUM(IIF(sbcuststatus = 'active', 1, 0)), 0) AS REAL) / COUNT(*)
  ) AS ar
FROM main.sbcustomer
WHERE
  sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
GROUP BY
  1
