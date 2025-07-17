WITH _s1 AS (
  SELECT
    COUNT(*) AS n_other_countries
  FROM main.COUNTRIES
)
SELECT
  COUNTRIES.co_name AS name,
  _s1.n_other_countries
FROM main.COUNTRIES AS COUNTRIES
CROSS JOIN _s1 AS _s1
ORDER BY
  COUNTRIES.co_name
