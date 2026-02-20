WITH "_S3" AS (
  SELECT
    COLUMN1 AS PART_SIZE,
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
    ON COLUMN1 <= PART.p_size
    AND PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      COLUMN1 + 4
    )
  GROUP BY
    COLUMN1
)
SELECT
  COLUMN1 AS part_size,
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
  ON COLUMN1 = "_S3".PART_SIZE
