SELECT
  MAX(conference.name) AS name,
  COALESCE(CASE WHEN COUNT(publication.cid) > 0 THEN COUNT(publication.cid) ELSE NULL END, 0) AS num_publications
FROM main.conference AS conference
LEFT JOIN main.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
