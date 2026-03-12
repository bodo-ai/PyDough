SELECT
  name AS oranization_name,
  oid AS organization_id
FROM main.organization
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.author AS author
    JOIN main.writes AS writes
      ON author.aid = writes.aid
    JOIN main.domain_publication AS domain_publication
      ON domain_publication.pid = writes.pid
    JOIN main.domain AS domain
      ON domain.did = domain_publication.did AND domain.name = 'Machine Learning'
    WHERE
      author.oid = organization.oid
  )
