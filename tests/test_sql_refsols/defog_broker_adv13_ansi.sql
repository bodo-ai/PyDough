WITH _t0 AS (
  SELECT
    COUNT() AS agg_0,
    sbcustcountry AS country
  FROM main.sbcustomer
  WHERE
    sbcustjoindate >= CAST('2023-01-01' AS DATE)
  GROUP BY
    sbcustcountry
)
SELECT
  country AS cust_country,
  COALESCE(agg_0, 0) AS TAC
FROM _t0
