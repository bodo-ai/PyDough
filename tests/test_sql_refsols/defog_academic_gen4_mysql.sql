WITH _s3 AS (
  SELECT
    domain_publication.did,
    AVG(publication.reference_num) AS avg_reference_num
  FROM domain_publication AS domain_publication
  JOIN publication AS publication
    ON domain_publication.pid = publication.pid
  GROUP BY
    1
)
SELECT
  domain.name,
  _s3.avg_reference_num AS average_references
FROM domain AS domain
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain.did
