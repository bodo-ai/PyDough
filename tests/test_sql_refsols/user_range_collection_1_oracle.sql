WITH "_S1" AS (
  SELECT
    p_size AS P_SIZE,
    COUNT(*) AS N_ROWS
  FROM TPCH.PART
  WHERE
    p_name LIKE '%turquoise%'
  GROUP BY
    p_size
)
SELECT
  COLUMN1 AS part_size,
  NVL("_S1".N_ROWS, 0) AS n_parts
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
LEFT JOIN "_S1" "_S1"
  ON COLUMN1 = "_S1".P_SIZE
