SELECT
  ARBITRARY(conference.name) AS name,
  COUNT(publication.cid) AS num_publications
FROM postgres.conference AS conference
LEFT JOIN postgres.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1 NULLS FIRST
