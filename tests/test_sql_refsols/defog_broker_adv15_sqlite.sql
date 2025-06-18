WITH _t0 AS (
  SELECT
    SUM(sbcuststatus = 'active') AS agg_0,
    COUNT(*) AS agg_1,
    sbcustcountry AS country
  FROM main.sbcustomer
  WHERE
    sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
  GROUP BY
    sbcustcountry
)
SELECT
  country,
  100 * COALESCE(CAST(COALESCE(agg_0, 0) AS REAL) / agg_1, 0.0) AS ar
FROM _t0
