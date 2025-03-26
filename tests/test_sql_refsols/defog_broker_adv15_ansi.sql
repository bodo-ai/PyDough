WITH _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(sbcustomer.sbcuststatus = 'active') AS agg_0,
    sbcustomer.sbcustcountry AS country
  FROM main.sbcustomer AS sbcustomer
  WHERE
    sbcustomer.sbcustjoindate <= '2022-12-31'
    AND sbcustomer.sbcustjoindate >= '2022-01-01'
  GROUP BY
    sbcustomer.sbcustcountry
)
SELECT
  _t0.country AS country,
  100 * COALESCE(COALESCE(_t0.agg_0, 0) / COALESCE(_t0.agg_1, 0), 0.0) AS ar
FROM _t0 AS _t0
