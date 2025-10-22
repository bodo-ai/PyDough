WITH _t1 AS (
  SELECT
    MAX(organization.continent) AS anything_continent,
    COUNT(author.oid) AS count_oid
  FROM main.organization AS organization
  LEFT JOIN main.author AS author
    ON author.oid = organization.oid
  GROUP BY
    organization.oid
)
SELECT
  anything_continent AS continent,
  CAST(COALESCE(SUM(NULLIF(count_oid, 0)), 0) AS REAL) / COUNT(*) AS ratio
FROM _t1
GROUP BY
  1
ORDER BY
  2 DESC
