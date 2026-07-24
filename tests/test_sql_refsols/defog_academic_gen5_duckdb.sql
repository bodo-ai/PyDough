SELECT
  year,
  AVG(citation_num) AS average_citations
FROM main.publication
GROUP BY
  1
