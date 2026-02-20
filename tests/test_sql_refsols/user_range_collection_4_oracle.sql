WITH "_T" AS (
  SELECT
    PART.p_name AS P_NAME,
    PART.p_retailprice AS P_RETAILPRICE,
    COLUMN1 AS PART_SIZE,
    ROW_NUMBER() OVER (PARTITION BY COLUMN1 ORDER BY PART.p_retailprice) AS "_W"
  FROM (VALUES
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7),
    (8),
    (9),
    (10)) AS SIZES(PART_SIZE)
  JOIN TPCH.PART PART
    ON COLUMN1 = PART.p_size
    AND PART.p_container LIKE '%SM DRUM%'
    AND PART.p_name LIKE '%azure%'
    AND PART.p_type LIKE '%PLATED%'
)
SELECT
  PART_SIZE AS part_size,
  P_NAME AS name,
  P_RETAILPRICE AS retail_price
FROM "_T"
WHERE
  "_W" = 1
ORDER BY
  1 NULLS FIRST
