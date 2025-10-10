WITH _t1 AS (
  SELECT
    continent,
    oid
  FROM main.organization
), _s2 AS (
  SELECT
    continent,
    COUNT(DISTINCT oid) AS ndistinct_oid
  FROM _t1
  GROUP BY
    1
), _s3 AS (
  SELECT
    _s0.continent,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT author.aid) AS ndistinct_aid
  FROM _t1 AS _s0
  JOIN main.author AS author
    ON _s0.oid = author.oid
  GROUP BY
    1
)
SELECT
  _s2.continent,
  IIF(
    (
      NOT _s3.n_rows IS NULL AND _s3.n_rows > 0
    ),
    CAST(COALESCE(_s3.ndistinct_aid, 0) AS REAL) / _s2.ndistinct_oid,
    0
  ) AS ratio
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.continent = _s3.continent
ORDER BY
  2 DESC
