SELECT
  title
FROM main.publication
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.writes AS writes
    JOIN main.domain_author AS domain_author
      ON domain_author.aid = writes.aid
    JOIN main.domain AS domain
      ON LOWER(domain.name) LIKE '%sociology%' AND domain.did = domain_author.did
    JOIN main.domain_conference AS domain_conference
      ON domain.did = domain_conference.did
    WHERE
      domain_conference.cid = publication.cid AND publication.pid = writes.pid
  )
  AND year = 2020
