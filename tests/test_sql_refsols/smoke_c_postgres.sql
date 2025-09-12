SELECT
  COUNT(*) AS a,
  COALESCE(SUM(FLOOR(c_acctbal)), 0) AS b,
  COALESCE(SUM(CEIL(c_acctbal)), 0) AS c,
  COUNT(DISTINCT c_mktsegment) AS d,
  ROUND(CAST(AVG(CAST(ABS(c_acctbal) AS DECIMAL)) AS DECIMAL), 4) AS e,
  MIN(c_acctbal) AS f,
  MAX(c_acctbal) AS g,
  MAX(SUBSTRING(c_name FROM 1 FOR 1)) AS h,
  COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) AS i,
  CEIL(VAR_POP(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)) AS j,
  ROUND(CAST(VAR_SAMP(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) AS DECIMAL), 4) AS k,
  FLOOR(STDDEV_POP(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)) AS l,
  ROUND(CAST(STDDEV(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) AS DECIMAL), 4) AS m,
  ROUND(
    CAST(AVG(
      CAST(COALESCE(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 0) AS DECIMAL)
    ) AS DECIMAL),
    2
  ) AS n,
  SUM(
    CASE
      WHEN NOT CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL
      THEN 1
      ELSE 0
    END
  ) AS o,
  SUM(
    CASE
      WHEN CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL
      THEN 1
      ELSE 0
    END
  ) AS p,
  PERCENTILE_DISC(0.2) WITHIN GROUP (ORDER BY
    c_acctbal) AS q,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    c_acctbal) AS r
FROM tpch.customer
