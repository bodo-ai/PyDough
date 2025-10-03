WITH _t0 AS (
  SELECT
    writes.aid,
    COUNT(DISTINCT domain_publication.did) AS ndistinct_did
  FROM main.writes AS writes
  JOIN main.publication AS publication
    ON publication.pid = writes.pid
  JOIN main.domain_publication AS domain_publication
    ON domain_publication.pid = publication.pid
  JOIN main.domain AS domain
    ON domain.did = domain_publication.did
    AND domain.name IN ('Data Science', 'Machine Learning')
  GROUP BY
    1
)
SELECT
  author.name
FROM main.author AS author
JOIN _t0 AS _t0
  ON _t0.aid = author.aid AND _t0.ndistinct_did = 2
