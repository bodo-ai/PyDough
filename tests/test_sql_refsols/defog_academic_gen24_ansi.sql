WITH _s7 AS (
  SELECT
    domain_conference.cid,
    writes.pid
  FROM main.writes AS writes
  JOIN main.domain_author AS domain_author
    ON domain_author.aid = writes.aid
  JOIN main.domain AS domain
    ON LOWER(domain.name) LIKE '%sociology%' AND domain.did = domain_author.did
  JOIN main.domain_conference AS domain_conference
    ON domain.did = domain_conference.did
)
SELECT
  publication.title
FROM main.publication AS publication
JOIN _s7 AS _s7
  ON _s7.cid = publication.cid AND _s7.pid = publication.pid
WHERE
  publication.year = 2020
