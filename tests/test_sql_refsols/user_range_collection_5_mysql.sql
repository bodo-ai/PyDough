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
), _s3 AS (
  SELECT
    _q_1.`1` AS part_size,
    COUNT(*) AS n_rows
  FROM _q_0 AS _q_1
  JOIN tpch.PART AS PART
    ON PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      _q_1.`1` + 4
    )
    AND PART.p_size >= _q_1.`1`
  GROUP BY
    1
)
SELECT
  _q_0.`1` AS part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM _q_0 AS _q_0
LEFT JOIN _s3 AS _s3
  ON _q_0.`1` = _s3.part_size
