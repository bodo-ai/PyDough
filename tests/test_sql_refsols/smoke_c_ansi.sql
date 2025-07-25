SELECT
  COUNT(*) AS a,
  COALESCE(SUM(FLOOR(c_acctbal)), 0) AS b,
  COALESCE(SUM(CEIL(c_acctbal)), 0) AS c,
  COUNT(DISTINCT c_mktsegment) AS d,
  ROUND(AVG(ABS(c_acctbal)), 4) AS e,
  MIN(c_acctbal) AS f,
  MAX(c_acctbal) AS g,
  ANY_VALUE(SUBSTRING(c_name, 1, 1)) AS h,
  COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) AS i,
  CEIL(VARIANCE_POP(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)) AS j,
  ROUND(VARIANCE(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END), 4) AS k,
  FLOOR(STDDEV_POP(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)) AS l,
  ROUND(STDDEV(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END), 4) AS m,
  ROUND(AVG(COALESCE(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 0)), 2) AS n,
  SUM(NOT CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL) AS o,
  SUM(CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL) AS p,
  PERCENTILE_DISC(0.2) WITHIN GROUP (ORDER BY
    c_acctbal NULLS LAST) AS q,
  MEDIAN(c_acctbal) AS r
FROM tpch.customer
