WITH _s1 AS (
  SELECT
    oid,
    COUNT(*) AS n_rows
  FROM main.author
  GROUP BY
    1
)
SELECT
  organization.continent,
  COALESCE(SUM(_s1.n_rows), 0) / COUNT(*) AS ratio
FROM main.organization AS organization
LEFT JOIN _s1 AS _s1
  ON _s1.oid = organization.oid
GROUP BY
  1
ORDER BY
  2 DESC
