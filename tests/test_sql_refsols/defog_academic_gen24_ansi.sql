WITH _s9 AS (
  SELECT
    domain_conference.cid,
    writes.pid
  FROM main.writes AS writes
  JOIN main.author AS author
    ON author.aid = writes.aid
  JOIN main.domain_author AS domain_author
    ON author.aid = domain_author.aid
  JOIN main.domain AS domain
    ON LOWER(domain.name) LIKE '%sociology%' AND domain.did = domain_author.did
  JOIN main.domain_conference AS domain_conference
    ON domain.did = domain_conference.did
)
SELECT
  publication.title
FROM main.publication AS publication
JOIN _s9 AS _s9
  ON _s9.cid = publication.cid AND _s9.pid = publication.pid
WHERE
  publication.year = 2020
