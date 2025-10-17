WITH _s2 AS (
  SELECT
    column1 AS part_size
  FROM (VALUES
    (1),
    (6),
    (11),
    (16),
    (21),
    (26),
    (31),
    (36),
    (41),
    (46),
    (51),
    (56),
    (61),
    (66),
    (71),
    (76),
    (81),
    (86),
    (91),
    (96)) AS sizes
), _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    _s0.part_size
  FROM _s2 AS _s0
  JOIN tpch.part AS part
    ON _s0.part_size = part.p_size AND part.p_name LIKE '%turquoise%'
  GROUP BY
    _s0.part_size
)
SELECT
  _s2.part_size,
  COALESCE(_s3.n_rows, 0) AS n_parts
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.part_size = _s3.part_size
