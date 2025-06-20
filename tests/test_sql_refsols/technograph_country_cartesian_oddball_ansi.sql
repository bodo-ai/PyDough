WITH _s3 AS (
  SELECT
    COUNT(*) AS n_other_countries
  FROM main.countries
)
SELECT
  _s0.co_name AS name,
  _s3.n_other_countries
FROM main.countries AS _s0
CROSS JOIN _s3 AS _s3
ORDER BY
  name
