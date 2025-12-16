WITH _t2 AS (
  SELECT
    ABS(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0)) AS expr_13,
    ROW_NUMBER() OVER (ORDER BY CASE WHEN sbcustname COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, sbcustname COLLATE utf8mb4_bin) AS `rank`,
    AVG(ABS(COALESCE(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0), 0))) OVER () AS ravg1,
    COALESCE(
      AVG(ABS(COALESCE(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0), 0))) OVER (ORDER BY sbcustname COLLATE utf8mb4_bin ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0.1
    ) AS ravg2,
    COUNT(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0)) OVER () AS rcnt1,
    COALESCE(
      COUNT(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0)) OVER (ORDER BY sbcustname COLLATE utf8mb4_bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0.1
    ) AS rcnt2,
    COUNT(*) OVER () AS rsiz1,
    COALESCE(
      COUNT(*) OVER (ORDER BY sbcustname COLLATE utf8mb4_bin ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
      0.1
    ) AS rsiz2,
    SUM(ABS(COALESCE(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0), 0))) OVER () AS rsum1,
    COALESCE(
      SUM(ABS(COALESCE(TRUNCATE(CAST(sbcustpostalcode AS FLOAT), 0), 0))) OVER (ORDER BY sbcustname COLLATE utf8mb4_bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0.1
    ) AS rsum2
  FROM main.sbCustomer
), _t1 AS (
  SELECT
    `rank`,
    ravg1,
    ravg2,
    rcnt1,
    rcnt2,
    rsiz1,
    rsiz2,
    rsum1,
    rsum2,
    CASE
      WHEN FLOOR(0.75 * COUNT(expr_13) OVER ()) < ROW_NUMBER() OVER (ORDER BY expr_13 DESC)
      THEN expr_13
      ELSE NULL
    END AS expr_15,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (ORDER BY expr_13 DESC) - 1.0
        ) - (
          (
            COUNT(expr_13) OVER () - 1.0
          ) / 2.0
        )
      ) < 1.0
      THEN expr_13
      ELSE NULL
    END AS expr_16
  FROM _t2
)
SELECT
  TRUE AS s00,
  TRUE AS s01,
  FALSE AS s02,
  FALSE AS s03,
  FALSE AS s04,
  FALSE AS s05,
  COUNT(*) >= 3 AS s06,
  FALSE AS s07,
  COUNT(*) <= 6 AS s08,
  FALSE AS s09,
  91 AS s10,
  0 AS s11,
  50 AS s12,
  35 AS s13,
  25.0 AS s14,
  ABS(COUNT(*) * -0.75) AS s15,
  10 AS s16,
  COUNT(*) AS s17,
  COUNT(*) AS s18,
  FALSE AS s19,
  TRUE AS s20,
  FALSE AS s21,
  TRUE AS s22,
  FALSE AS s23,
  TRUE AS s24,
  MAX(expr_15) AS s25,
  AVG(expr_16) AS s26,
  MIN(`rank`) AS s27,
  MAX(`rank`) AS s28,
  ANY_VALUE(rsum1) AS s29,
  ROUND(SUM(rsum2), 2) AS s30,
  ANY_VALUE(ravg1) AS s31,
  ROUND(SUM(ravg2), 2) AS s32,
  ANY_VALUE(rcnt1) AS s33,
  ROUND(SUM(rcnt2), 2) AS s34,
  ANY_VALUE(rsiz1) AS s35,
  ROUND(SUM(rsiz2), 2) AS s36
FROM _t1
