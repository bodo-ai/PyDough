WITH _t1 AS (
  SELECT
    c_acctbal,
    c_mktsegment,
    c_name,
    CASE
      WHEN 0.8 * COUNT(c_acctbal) OVER () < ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS LAST)
      THEN c_acctbal
      ELSE NULL
    END AS expr_30,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY c_acctbal DESC NULLS LAST) - 1.0
        ) - (
          CAST((
            COUNT(c_acctbal) OVER () - 1.0
          ) AS DOUBLE PRECISION) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_31
  FROM tpch.customer
)
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
  MAX(expr_30) AS q,
  AVG(CAST(expr_31 AS DECIMAL)) AS r
FROM _t1
