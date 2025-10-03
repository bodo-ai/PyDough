WITH _s3 AS (
  SELECT
    SUM(publication.reference_num) / SUM(CASE WHEN NOT publication.reference_num IS NULL THEN 1 ELSE 0 END) AS avg_reference_num,
    domain_publication.did
  FROM main.domain_publication AS domain_publication
  JOIN main.publication AS publication
    ON domain_publication.pid = publication.pid
  GROUP BY
    2
)
SELECT
  domain.name,
  _s3.avg_reference_num AS average_references
FROM main.domain AS domain
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain.did
