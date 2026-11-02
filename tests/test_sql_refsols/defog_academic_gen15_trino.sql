WITH _s2 AS (
  SELECT
    continent,
    COUNT(*) AS n_rows
  FROM cassandra.defog.organization
  GROUP BY
    1
), _s3 AS (
  SELECT
    organization.continent,
    COUNT(*) AS n_rows
  FROM cassandra.defog.organization AS organization
  JOIN postgres.main.author AS author
    ON author.oid = organization.oid
  GROUP BY
    1
)
SELECT
  _s2.continent,
  CAST(COALESCE(_s3.n_rows, 0) AS DOUBLE) / NULLIF(_s2.n_rows, 0) AS ratio
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.continent = _s3.continent
ORDER BY
  2 DESC
