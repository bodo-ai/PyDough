WITH _t1 AS (
  SELECT
    c_acctbal,
    c_mktsegment,
    c_name,
    CASE
      WHEN CAST(0.8 * COUNT(c_acctbal) OVER () AS INTEGER) < ROW_NUMBER() OVER (ORDER BY c_acctbal DESC)
      THEN c_acctbal
      ELSE NULL
    END AS expr_30,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(c_acctbal) OVER () - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN c_acctbal
      ELSE NULL
    END AS expr_31
  FROM tpch.customer
)
SELECT
  COUNT(*) AS a,
  COALESCE(
    SUM(
      CAST(c_acctbal AS INTEGER) - CASE WHEN c_acctbal < CAST(c_acctbal AS INTEGER) THEN 1 ELSE 0 END
    ),
    0
  ) AS b,
  COALESCE(
    SUM(
      CAST(c_acctbal AS INTEGER) + CASE WHEN c_acctbal > CAST(c_acctbal AS INTEGER) THEN 1 ELSE 0 END
    ),
    0
  ) AS c,
  COUNT(DISTINCT c_mktsegment) AS d,
  ROUND(AVG(ABS(c_acctbal)), 4) AS e,
  MIN(c_acctbal) AS f,
  MAX(c_acctbal) AS g,
  MAX(SUBSTRING(c_name, 1, 1)) AS h,
  COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) AS i,
  CAST((
    CAST((
      SUM((
        POWER(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 2)
      )) - (
        CAST((
          POWER(SUM(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END), 2)
        ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
      )
    ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
  ) AS INTEGER) + CASE
    WHEN (
      CAST((
        SUM((
          POWER(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          CAST((
            POWER(SUM(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END), 2)
          ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
        )
      ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
    ) > CAST((
      CAST((
        SUM((
          POWER(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          CAST((
            POWER(SUM(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END), 2)
          ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
        )
      ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
    ) AS INTEGER)
    THEN 1
    ELSE 0
  END AS j,
  ROUND(
    (
      CAST((
        SUM((
          POWER(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          CAST((
            POWER(SUM(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END), 2)
          ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
        )
      ) AS REAL) / (
        COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END) - 1
      )
    ),
    4
  ) AS k,
  CAST(POWER(
    (
      CAST((
        SUM((
          POWER(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END, 2)
        )) - (
          CAST((
            POWER(SUM(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END), 2)
          ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
        )
      ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
    ),
    0.5
  ) AS INTEGER) - CASE
    WHEN CAST(POWER(
      (
        CAST((
          SUM((
            POWER(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END, 2)
          )) - (
            CAST((
              POWER(SUM(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END), 2)
            ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
          )
        ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
      ),
      0.5
    ) AS INTEGER) > POWER(
      (
        CAST((
          SUM((
            POWER(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END, 2)
          )) - (
            CAST((
              POWER(SUM(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END), 2)
            ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
          )
        ) AS REAL) / COUNT(CASE WHEN c_acctbal < 0 THEN c_acctbal ELSE NULL END)
      ),
      0.5
    )
    THEN 1
    ELSE 0
  END AS l,
  ROUND(
    POWER(
      (
        CAST((
          SUM((
            POWER(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 2)
          )) - (
            CAST((
              POWER(SUM(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END), 2)
            ) AS REAL) / COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END)
          )
        ) AS REAL) / (
          COUNT(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END) - 1
        )
      ),
      0.5
    ),
    4
  ) AS m,
  ROUND(AVG(COALESCE(CASE WHEN c_acctbal > 0 THEN c_acctbal ELSE NULL END, 0)), 2) AS n,
  SUM(NOT CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL) AS o,
  SUM(CASE WHEN c_acctbal > 1000 THEN c_acctbal ELSE NULL END IS NULL) AS p,
  MAX(expr_30) AS q,
  AVG(expr_31) AS r
FROM _t1
