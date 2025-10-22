WITH _s1 AS (
  SELECT
    oid
  FROM main.author
), _t1 AS (
  SELECT
    ANY_VALUE(organization.continent) AS anything_continent,
    COUNT(_s1.oid) AS count_oid
  FROM main.organization AS organization
  LEFT JOIN _s1 AS _s1
    ON _s1.oid = organization.oid
  GROUP BY
    organization.oid
)
SELECT
  anything_continent AS continent,
  SUM(count_oid) / COUNT(*) AS ratio
FROM _t1
GROUP BY
  1
ORDER BY
  2 DESC NULLS LAST
