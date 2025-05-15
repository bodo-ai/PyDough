WITH _t0 AS (
  SELECT
    ANY_VALUE(countries.co_name) AS name,
    COUNT() AS n_other_countries
  FROM main.countries AS countries
  CROSS JOIN main.countries AS countries_2
  GROUP BY
    countries.co_id
)
SELECT
  name,
  n_other_countries
FROM _t0
ORDER BY
  name
