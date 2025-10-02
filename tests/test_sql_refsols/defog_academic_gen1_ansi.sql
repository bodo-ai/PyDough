SELECT
  publication.title
FROM main.writes AS writes
JOIN main.publication AS publication
  ON publication.pid = writes.pid
