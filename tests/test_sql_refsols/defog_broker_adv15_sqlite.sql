WITH _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sbcuststatus = 'active') AS agg_0,
    sbcustcountry AS country
  FROM main.sbcustomer
  WHERE
    sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
  GROUP BY
    sbcustcountry
)
SELECT
  country AS country,
  100 * COALESCE(CAST(COALESCE(agg_0, 0) AS REAL) / COALESCE(agg_1, 0), 0.0) AS ar
FROM _t0
