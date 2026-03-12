WITH _u_0 AS (
  SELECT
    domain_conference.cid AS _u_1,
    writes.pid AS _u_2
  FROM academic.writes AS writes
  JOIN academic.domain_author AS domain_author
    ON domain_author.aid = writes.aid
  JOIN academic.domain AS domain
    ON CONTAINS(LOWER(domain.name), 'sociology') AND domain.did = domain_author.did
  JOIN academic.domain_conference AS domain_conference
    ON domain.did = domain_conference.did
  GROUP BY
    1,
    2
)
SELECT
  publication.title
FROM academic.publication AS publication
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = publication.cid AND _u_0._u_2 = publication.pid
WHERE
  NOT _u_0._u_1 IS NULL AND publication.year = 2020
