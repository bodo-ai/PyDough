SELECT
  organization.name AS oranization_name,
  organization.oid AS organization_id
FROM main.organization AS organization
JOIN main.author AS author
  ON author.oid = organization.oid
JOIN main.writes AS writes
  ON author.aid = writes.aid
JOIN main.domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN main.domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Machine Learning'
