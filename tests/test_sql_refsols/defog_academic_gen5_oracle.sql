SELECT
  year,
  AVG(citation_num) AS average_citations
FROM MAIN.PUBLICATION
GROUP BY
  year
