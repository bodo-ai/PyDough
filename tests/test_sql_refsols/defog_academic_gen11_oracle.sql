WITH "_s0" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM MAIN.PUBLICATION
), "_s1" AS (
  SELECT
    COUNT(*) AS N_ROWS
  FROM MAIN.AUTHOR
)
SELECT
  "_s0".N_ROWS / NULLIF("_s1".N_ROWS, 0) AS publication_to_author_ratio
FROM "_s0" "_s0"
CROSS JOIN "_s1" "_s1"
