WITH _t0 AS (
  SELECT
    SUM(l_extendedprice * l_discount) AS agg_0
  FROM tpch.lineitem
  WHERE
    l_discount <= 0.07
    AND l_discount >= 0.05
    AND l_quantity < 24
    AND l_shipdate < '1995-01-01'
    AND l_shipdate >= '1994-01-01'
)
SELECT
  COALESCE(agg_0, 0) AS REVENUE
FROM _t0
