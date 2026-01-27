SELECT
  ANY_VALUE(conference.name) COLLATE utf8mb4_bin AS name,
  COUNT(publication.cid) AS num_publications
FROM conference AS conference
LEFT JOIN publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1
