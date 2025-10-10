WITH _s1 AS (
  SELECT
    did,
    COUNT(*) AS n_rows
  FROM main.domain_keyword
  GROUP BY
    1
), _s3 AS (
  SELECT
    domain.did,
    _s1.n_rows
  FROM main.domain AS domain
  JOIN _s1 AS _s1
    ON _s1.did = domain.did
)
SELECT
  domain_publication.did AS domain_id,
  IIF(
    (
      NOT SUM(_s3.n_rows) IS NULL AND SUM(_s3.n_rows) > 0
    ),
    CAST(COUNT(*) AS REAL) / COALESCE(SUM(_s3.n_rows), 0),
    NULL
  ) AS ratio
FROM main.domain_publication AS domain_publication
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain_publication.did
GROUP BY
  1
