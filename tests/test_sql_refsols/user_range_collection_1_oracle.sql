WITH "_s1" AS (
  SELECT
    P_SIZE,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    P_NAME LIKE '%turquoise%'
  GROUP BY
    P_SIZE
)
SELECT
  SIZES.PART_SIZE AS part_size,
  COALESCE("_s1".N_ROWS, 0) AS n_parts
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
  (96)) AS SIZES(PART_SIZE)
LEFT JOIN "_s1" "_s1"
  ON SIZES.PART_SIZE = "_s1".P_SIZE
