SELECT
  year,
  AVG(citation_num) AS average_citations
FROM academic.publication
GROUP BY
  1
