WITH _s3 AS (
  SELECT
    writes.aid,
    COUNT(DISTINCT publication.pid) AS ndistinct_pid
  FROM main.writes AS writes
  JOIN main.publication AS publication
    ON publication.pid = writes.pid AND publication.year = 2021
  GROUP BY
    1
)
SELECT
  author.name,
  _s3.ndistinct_pid AS count_publication
FROM main.author AS author
JOIN _s3 AS _s3
  ON _s3.aid = author.aid
ORDER BY
  2 DESC
LIMIT 1
