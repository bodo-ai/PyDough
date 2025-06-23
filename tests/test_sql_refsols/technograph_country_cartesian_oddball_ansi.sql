WITH _s1 AS (
  SELECT
    COUNT(*) AS n_other_countries
  FROM main.countries
)
SELECT
  countries.co_name AS name,
  _s1.n_other_countries
FROM main.countries AS countries
CROSS JOIN _s1 AS _s1
ORDER BY
  countries.co_name
