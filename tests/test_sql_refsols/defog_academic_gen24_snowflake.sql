SELECT
  title
FROM academic.publication
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM academic.writes AS writes
    JOIN academic.domain_author AS domain_author
      ON domain_author.aid = writes.aid
    JOIN academic.domain AS domain
      ON CONTAINS(LOWER(domain.name), 'sociology') AND domain.did = domain_author.did
    JOIN academic.domain_conference AS domain_conference
      ON domain.did = domain_conference.did
    WHERE
      domain_conference.cid = publication.cid AND publication.pid = writes.pid
  )
  AND year = 2020
