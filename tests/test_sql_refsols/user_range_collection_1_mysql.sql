WITH _q_0 AS (
  SELECT
    1 AS `1`
  UNION ALL
  SELECT
    6 AS `6`
  UNION ALL
  SELECT
    11 AS `11`
  UNION ALL
  SELECT
    16 AS `16`
  UNION ALL
  SELECT
    21 AS `21`
  UNION ALL
  SELECT
    26 AS `26`
  UNION ALL
  SELECT
    31 AS `31`
  UNION ALL
  SELECT
    36 AS `36`
  UNION ALL
  SELECT
    41 AS `41`
  UNION ALL
  SELECT
    46 AS `46`
  UNION ALL
  SELECT
    51 AS `51`
  UNION ALL
  SELECT
    56 AS `56`
  UNION ALL
  SELECT
    61 AS `61`
  UNION ALL
  SELECT
    66 AS `66`
  UNION ALL
  SELECT
    71 AS `71`
  UNION ALL
  SELECT
    76 AS `76`
  UNION ALL
  SELECT
    81 AS `81`
  UNION ALL
  SELECT
    86 AS `86`
  UNION ALL
  SELECT
    91 AS `91`
  UNION ALL
  SELECT
    96 AS `96`
), _s1 AS (
  SELECT
    p_size,
    COUNT(*) AS n_rows
  FROM tpch.PART
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    1
)
SELECT
  _q_0.`1` AS part_size,
  COALESCE(_s1.n_rows, 0) AS n_parts
FROM _q_0 AS _q_0
LEFT JOIN _s1 AS _s1
  ON _q_0.`1` = _s1.p_size
