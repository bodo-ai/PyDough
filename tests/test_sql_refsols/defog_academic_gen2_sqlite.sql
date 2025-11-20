WITH _s3 AS (
  SELECT
    writes.aid,
    SUM(publication.citation_num) AS sum_citationnum
  FROM main.writes AS writes
  JOIN main.publication AS publication
    ON publication.pid = writes.pid
  GROUP BY
    1
)
SELECT
  author.name,
  COALESCE(_s3.sum_citationnum, 0) AS total_citations
FROM main.author AS author
JOIN _s3 AS _s3
  ON _s3.aid = author.aid
