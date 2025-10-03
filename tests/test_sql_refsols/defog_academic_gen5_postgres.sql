SELECT
  year,
  AVG(CAST(citation_num AS DECIMAL)) AS average_citations
FROM main.publication
GROUP BY
  1
