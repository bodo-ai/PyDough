WITH _s7 AS (
  SELECT
    author.oid
  FROM main.author AS author
  JOIN main.writes AS writes
    ON author.aid = writes.aid
  JOIN main.domain_publication AS domain_publication
    ON domain_publication.pid = writes.pid
  JOIN main.domain AS domain
    ON domain.did = domain_publication.did AND domain.name = 'Machine Learning'
)
SELECT
  organization.name AS oranization_name,
  organization.oid AS organization_id
FROM main.organization AS organization
SEMI JOIN _s7 AS _s7
  ON _s7.oid = organization.oid
