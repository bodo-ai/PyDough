SELECT
  ANY_VALUE(conference.name) AS name,
  COUNT(publication.cid) AS count_publications
FROM academic.conference AS conference
LEFT JOIN academic.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC NULLS LAST,
  1 DESC NULLS LAST
