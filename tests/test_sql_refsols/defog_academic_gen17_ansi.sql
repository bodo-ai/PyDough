SELECT
  ANY_VALUE(conference.name) AS name,
  COALESCE(NULLIF(COUNT(publication.cid), 0), 0) AS count_publications
FROM main.conference AS conference
LEFT JOIN main.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1 DESC
