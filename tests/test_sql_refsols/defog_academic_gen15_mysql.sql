WITH _t1 AS (
  SELECT
    ANY_VALUE(organization.continent) AS anything_continent,
    COUNT(author.oid) AS count_oid
  FROM organization AS organization
  LEFT JOIN author AS author
    ON author.oid = organization.oid
  GROUP BY
    organization.oid
)
SELECT
  anything_continent AS continent,
  COALESCE(SUM(count_oid), 0) / COUNT(*) AS ratio
FROM _t1
GROUP BY
  1
ORDER BY
  2 DESC
