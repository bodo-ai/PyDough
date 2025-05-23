WITH _s1 AS (
  SELECT
    COUNT() AS agg_0
  FROM main.countries
)
SELECT
  countries.co_name AS name,
  COALESCE(_s1.agg_0, 0) AS n_other_countries
FROM main.countries AS countries
LEFT JOIN _s1 AS _s1
  ON TRUE
ORDER BY
  name
