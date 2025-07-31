WITH _t1 AS (
  SELECT
    CASE
      WHEN CAST(0.8 * COUNT(c_acctbal) OVER () AS SIGNED) < ROW_NUMBER() OVER (ORDER BY c_acctbal DESC)
      THEN c_acctbal
      ELSE NULL
    END AS expr_30,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) - 1.0
        ) - (
          (
            COUNT(c_acctbal) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_31,
    c_acctbal,
    c_mktsegment,
    c_name
  FROM tpch.CUSTOMER
)
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
  MAX(expr_30) AS q,
  AVG(expr_31) AS r
FROM _t1
