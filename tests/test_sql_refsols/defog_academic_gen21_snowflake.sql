WITH _u_0 AS (
  SELECT
    author.oid AS _u_1
  FROM academic.author AS author
  JOIN academic.writes AS writes
    ON author.aid = writes.aid
  JOIN academic.domain_publication AS domain_publication
    ON domain_publication.pid = writes.pid
  JOIN academic.domain AS domain
    ON domain.did = domain_publication.did AND domain.name = 'Machine Learning'
  GROUP BY
    1
)
SELECT
  organization.name AS oranization_name,
  organization.oid AS organization_id
FROM academic.organization AS organization
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = organization.oid
WHERE
  NOT _u_0._u_1 IS NULL
