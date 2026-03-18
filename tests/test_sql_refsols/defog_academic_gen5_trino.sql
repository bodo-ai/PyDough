SELECT
  year,
  AVG(citation_num) AS average_citations
FROM postgres.publication
GROUP BY
  1
