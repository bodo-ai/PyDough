WITH _t0 AS (
  SELECT
    writes.aid,
    COUNT(DISTINCT domain_publication.did) AS ndistinct_did
  FROM writes AS writes
  JOIN domain_publication AS domain_publication
    ON domain_publication.pid = writes.pid
  JOIN domain AS domain
    ON domain.did = domain_publication.did
    AND domain.name IN ('Data Science', 'Machine Learning')
  GROUP BY
    1
)
SELECT
  author.name
FROM author AS author
JOIN _t0 AS _t0
  ON _t0.aid = author.aid AND _t0.ndistinct_did = 2
