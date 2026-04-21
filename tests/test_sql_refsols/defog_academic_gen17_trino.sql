SELECT
  ARBITRARY(conference.name) AS name,
  COUNT(publication.cid) AS count_publications
FROM cassandra.defog.conference AS conference
LEFT JOIN postgres.main.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1 DESC
