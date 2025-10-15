WITH _u_0 AS (
  SELECT
    author.oid AS _u_1
  FROM main.author AS author
  JOIN main.writes AS writes
    ON author.aid = writes.aid
  JOIN main.publication AS publication
    ON publication.pid = writes.pid
  JOIN main.domain_publication AS domain_publication
    ON domain_publication.pid = publication.pid
  JOIN main.domain AS domain
    ON domain.did = domain_publication.did AND domain.name = 'Machine Learning'
  GROUP BY
    1
)
SELECT
  organization.name AS oranization_name,
  organization.oid AS organization_id
FROM main.organization AS organization
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = organization.oid
WHERE
  NOT _u_0._u_1 IS NULL
