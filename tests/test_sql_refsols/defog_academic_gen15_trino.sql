WITH _t1 AS (
  SELECT
    ARBITRARY(organization.continent) AS anything_continent,
    COUNT(author.oid) AS count_oid
  FROM cassandra.defog.organization AS organization
  LEFT JOIN postgres.main.author AS author
    ON author.oid = organization.oid
  GROUP BY
    organization.oid
)
SELECT
  anything_continent AS continent,
  CAST(COALESCE(SUM(count_oid), 0) AS DOUBLE) / COUNT(*) AS ratio
FROM _t1
GROUP BY
  1
ORDER BY
  2 DESC
