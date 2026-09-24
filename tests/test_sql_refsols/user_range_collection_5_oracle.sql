WITH "_s3" AS (
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
    ON PART.P_NAME LIKE '%almond%'
    AND PART.P_SIZE <= (
      SIZES_2.PART_SIZE + 4
    )
    AND PART.P_SIZE >= SIZES_2.PART_SIZE
  GROUP BY
    SIZES_2.PART_SIZE
)
SELECT
  SIZES.PART_SIZE AS part_size,
  COALESCE("_s3".N_ROWS, 0) AS n_parts
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
LEFT JOIN "_s3" "_s3"
  ON SIZES.PART_SIZE = "_s3".PART_SIZE
