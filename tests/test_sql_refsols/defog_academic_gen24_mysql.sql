SELECT
  title
FROM publication
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM writes AS writes
    JOIN domain_author AS domain_author
      ON domain_author.aid = writes.aid
    JOIN domain AS domain
      ON LOWER(domain.name) LIKE '%sociology%' AND domain.did = domain_author.did
    JOIN domain_conference AS domain_conference
      ON domain.did = domain_conference.did
    WHERE
      domain_conference.cid = publication.cid AND publication.pid = writes.pid
  )
  AND year = 2020
