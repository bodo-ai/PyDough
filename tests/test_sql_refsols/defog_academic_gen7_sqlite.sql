WITH _s2 AS (
  SELECT DISTINCT
    name
  FROM main.domain
), _s3 AS (
  SELECT
    domain.name,
    COUNT(DISTINCT domain_author.aid) AS ndistinct_aid
  FROM main.domain AS domain
  JOIN main.domain_author AS domain_author
    ON domain.did = domain_author.did
  GROUP BY
    1
), _t0 AS (
  SELECT
    _s2.name,
    _s3.ndistinct_aid
  FROM _s2 AS _s2
  LEFT JOIN _s3 AS _s3
    ON _s2.name = _s3.name
  ORDER BY
    COALESCE(ndistinct_aid, 0)
  LIMIT 5
)
SELECT
  name,
  COALESCE(ndistinct_aid, 0) AS author_count
FROM _t0
ORDER BY
  2 DESC,
  1 DESC
