WITH _s3 AS (
  SELECT
    domain_publication.did,
    AVG(CAST(publication.reference_num AS DECIMAL)) AS avg_referencenum
  FROM main.domain_publication AS domain_publication
  JOIN main.publication AS publication
    ON domain_publication.pid = publication.pid
  GROUP BY
    1
)
SELECT
  domain.name,
  _s3.avg_referencenum AS average_references
FROM main.domain AS domain
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain.did
