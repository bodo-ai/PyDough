WITH "_S2" AS (
  SELECT
    COLUMN1 AS PART_SIZE
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
), "_S3" AS (
  SELECT
    "_S0".PART_SIZE,
    COUNT(*) AS N_ROWS
  FROM "_S2" "_S0"
  JOIN TPCH.PART PART
    ON PART.p_name LIKE '%almond%'
    AND PART.p_size <= (
      "_S0".PART_SIZE + 4
    )
    AND PART.p_size >= "_S0".PART_SIZE
  GROUP BY
    "_S0".PART_SIZE
)
SELECT
  "_S2".PART_SIZE AS part_size,
  COALESCE("_S3".N_ROWS, 0) AS n_parts
FROM "_S2" "_S2"
LEFT JOIN "_S3" "_S3"
  ON "_S2".PART_SIZE = "_S3".PART_SIZE
