WITH "_S3" AS (
  SELECT
    SIZES_2.PART_SIZE,
    COUNT(*) AS N_ROWS
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
    (56)) AS SIZES_2(PART_SIZE)
  JOIN TPCH.PART PART
    ON PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      SIZES_2.PART_SIZE + 4
    )
    AND PART.p_size >= SIZES_2.PART_SIZE
  GROUP BY
    SIZES_2.PART_SIZE
)
SELECT
  SIZES.PART_SIZE AS part_size,
  COALESCE("_S3".N_ROWS, 0) AS n_parts
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
  (56)) AS SIZES(PART_SIZE)
LEFT JOIN "_S3" "_S3"
  ON SIZES.PART_SIZE = "_S3".PART_SIZE
