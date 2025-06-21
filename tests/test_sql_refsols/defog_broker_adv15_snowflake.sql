WITH _T0 AS (
  SELECT
    COUNT_IF(sbcuststatus = 'active') AS AGG_0,
    COUNT(*) AS AGG_1,
    sbcustcountry AS COUNTRY
  FROM MAIN.SBCUSTOMER
  WHERE
    sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
  GROUP BY
    sbcustcountry
)
SELECT
  COUNTRY AS country,
  100 * COALESCE(COALESCE(AGG_0, 0) / AGG_1, 0.0) AS ar
FROM _T0
