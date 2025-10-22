WITH _s1 AS (
  SELECT
    cid
  FROM main.publication
)
SELECT
  ANY_VALUE(conference.name) COLLATE utf8mb4_bin AS name,
  COUNT(_s1.cid) AS count_publications
FROM main.conference AS conference
LEFT JOIN _s1 AS _s1
  ON _s1.cid = conference.cid
GROUP BY
  conference.cid
ORDER BY
  2 DESC,
  1 DESC
