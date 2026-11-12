SELECT
  year,
  AVG(CAST(citation_num AS DOUBLE)) AS average_citations
FROM postgres.main.publication
GROUP BY
  1
