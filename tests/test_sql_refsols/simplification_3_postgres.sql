WITH _t1 AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY sbcustname) AS rank,
    sbcustpostalcode,
    AVG(ABS(COALESCE(CAST(sbcustpostalcode AS INT), 0))) OVER () AS ravg1,
    COALESCE(
      AVG(ABS(COALESCE(CAST(sbcustpostalcode AS INT), 0))) OVER (ORDER BY sbcustname ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      0.1
    ) AS ravg2,
    COUNT(CAST(sbcustpostalcode AS INT)) OVER () AS rcnt1,
    COALESCE(
      COUNT(CAST(sbcustpostalcode AS INT)) OVER (ORDER BY sbcustname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0.1
    ) AS rcnt2,
    COUNT(*) OVER () AS rsiz1,
    COALESCE(
      COUNT(*) OVER (ORDER BY sbcustname ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING),
      0.1
    ) AS rsiz2,
    SUM(ABS(COALESCE(CAST(sbcustpostalcode AS INT), 0))) OVER () AS rsum1,
    COALESCE(
      SUM(ABS(COALESCE(CAST(sbcustpostalcode AS INT), 0))) OVER (ORDER BY sbcustname ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
      0.1
    ) AS rsum2
  FROM main.sbcustomer
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
  PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY
    ABS(CAST(sbcustpostalcode AS INT))) AS s25,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
    ABS(CAST(sbcustpostalcode AS INT))) AS s26,
  MIN(rank) AS s27,
  MAX(rank) AS s28,
  MAX(rsum1) AS s29,
  ROUND(CAST(SUM(rsum2) AS DECIMAL), 2) AS s30,
  MAX(ravg1) AS s31,
  ROUND(CAST(SUM(ravg2) AS DECIMAL), 2) AS s32,
  MAX(rcnt1) AS s33,
  ROUND(CAST(SUM(rcnt2) AS DECIMAL), 2) AS s34,
  MAX(rsiz1) AS s35,
  ROUND(CAST(SUM(rsiz2) AS DECIMAL), 2) AS s36
FROM _t1
