WITH _s1 AS (
  SELECT
    did,
    COUNT(*) AS n_rows
  FROM main.domain_publication
  GROUP BY
    1
), _s3 AS (
  SELECT
    did,
    COUNT(*) AS n_rows
  FROM main.domain_keyword
  GROUP BY
    1
)
SELECT
  domain.did AS domain_id,
  COALESCE(_s1.n_rows, 0) / CASE
    WHEN (
      NOT _s3.n_rows IS NULL AND _s3.n_rows > 0
    )
    THEN COALESCE(_s3.n_rows, 0)
    ELSE NULL
  END AS ratio
FROM main.domain AS domain
LEFT JOIN _s1 AS _s1
  ON _s1.did = domain.did
LEFT JOIN _s3 AS _s3
  ON _s3.did = domain.did
