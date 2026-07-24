SELECT
  year,
  AVG(citation_num) AS average_citations
FROM defog.academic.publication
GROUP BY
  1
