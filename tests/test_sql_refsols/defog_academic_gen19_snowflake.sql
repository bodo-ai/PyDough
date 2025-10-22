WITH _s1 AS (
  SELECT
    cid
  FROM main.publication
)
SELECT
  ANY_VALUE(conference.name) AS name,
  COUNT(_s1.cid) AS num_publications
FROM main.conference AS conference
LEFT JOIN _s1 AS _s1
  ON _s1.cid = conference.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC NULLS LAST,
  1 NULLS FIRST
