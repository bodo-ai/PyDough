WITH _s1 AS (
  SELECT
    cid,
    COUNT(*) AS n_rows
  FROM main.publication
  GROUP BY
    1
)
SELECT
  conference.name,
  COALESCE(_s1.n_rows, 0) AS num_publications
FROM main.conference AS conference
LEFT JOIN _s1 AS _s1
  ON _s1.cid = conference.cid
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
