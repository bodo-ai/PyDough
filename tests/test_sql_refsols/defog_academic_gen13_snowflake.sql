WITH _s3 AS (
  SELECT
    domain.did,
    COUNT(*) AS n_rows
  FROM main.domain AS domain
  JOIN main.domain_keyword AS domain_keyword
    ON domain.did = domain_keyword.did
  GROUP BY
    1
)
SELECT
  domain_publication.did AS domain_id,
  IFF(
    (
      NOT SUM(_s3.n_rows) IS NULL AND SUM(_s3.n_rows) > 0
    ),
    COUNT(*) / COALESCE(SUM(_s3.n_rows), 0),
    NULL
  ) AS ratio
FROM main.domain_publication AS domain_publication
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain_publication.did
GROUP BY
  1
