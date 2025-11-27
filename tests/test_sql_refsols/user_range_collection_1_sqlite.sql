WITH _s1 AS (
  SELECT
    p_size,
    COUNT(*) AS n_rows
  FROM tpch.part
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    1
)
SELECT
  column1 AS part_size,
  COALESCE(_s1.n_rows, 0) AS n_parts
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
  (96)) AS _q_0
LEFT JOIN _s1 AS _s1
  ON _s1.p_size = column1
