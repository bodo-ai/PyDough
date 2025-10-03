WITH _s0 AS (
  SELECT
    COUNT(DISTINCT pid) AS ndistinct_pid
  FROM main.publication
), _s1 AS (
  SELECT
    COUNT(DISTINCT aid) AS ndistinct_aid
  FROM main.author
)
SELECT
  CAST(_s0.ndistinct_pid AS DOUBLE PRECISION) / _s1.ndistinct_aid AS publication_to_author_ratio
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
