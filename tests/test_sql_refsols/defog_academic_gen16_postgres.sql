WITH _s1 AS (
  SELECT
    aid,
    name
  FROM main.author
), _s6 AS (
  SELECT DISTINCT
    _s1.name
  FROM main.writes AS writes
  JOIN _s1 AS _s1
    ON _s1.aid = writes.aid
), _s7 AS (
  SELECT
    _s3.name,
    COUNT(DISTINCT writes.pid) AS ndistinct_pid
  FROM main.writes AS writes
  JOIN _s1 AS _s3
    ON _s3.aid = writes.aid
  JOIN main.publication AS publication
    ON publication.pid = writes.pid AND publication.year = 2021
  GROUP BY
    1
)
SELECT
  _s6.name,
  COALESCE(_s7.ndistinct_pid, 0) AS count_publication
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.name = _s7.name
ORDER BY
  2 DESC NULLS LAST
LIMIT 1
