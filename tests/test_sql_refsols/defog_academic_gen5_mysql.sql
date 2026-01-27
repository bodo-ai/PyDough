SELECT
  year,
  AVG(citation_num) AS average_citations
FROM publication
GROUP BY
  1
