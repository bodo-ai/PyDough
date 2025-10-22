SELECT
  ANY_VALUE(conference.name) COLLATE utf8mb4_bin AS name,
  COALESCE(CASE WHEN COUNT(publication.cid) <> 0 THEN COUNT(publication.cid) ELSE NULL END, 0) AS num_publications
FROM main.conference AS conference
LEFT JOIN main.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1
