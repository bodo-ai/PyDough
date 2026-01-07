WITH _s1 AS (
  SELECT
    did,
    COUNT(DISTINCT aid) AS ndistinct_aid
  FROM main.domain_author
  GROUP BY
    1
)
SELECT
  domain.name COLLATE utf8mb4_bin AS name,
  COALESCE(_s1.ndistinct_aid, 0) AS author_count
FROM main.domain AS domain
LEFT JOIN _s1 AS _s1
  ON _s1.did = domain.did
ORDER BY
  2 DESC,
  1 DESC
LIMIT 5
