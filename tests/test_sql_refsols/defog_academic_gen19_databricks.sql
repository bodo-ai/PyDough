SELECT
  ANY_VALUE(conference.name) AS name,
  COUNT(publication.cid) AS num_publications
FROM defog.academic.conference AS conference
LEFT JOIN defog.academic.publication AS publication
  ON conference.cid = publication.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1
