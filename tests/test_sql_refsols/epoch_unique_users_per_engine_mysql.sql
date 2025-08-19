WITH _s2 AS (
  SELECT DISTINCT
    search_engine
  FROM SEARCHES
), _s3 AS (
  SELECT
    COUNT(DISTINCT USERS.user_id) AS ndistinct_user_id,
    SEARCHES.search_engine
  FROM SEARCHES AS SEARCHES
  JOIN USERS AS USERS
    ON SEARCHES.search_user_id = USERS.user_id
  WHERE
    EXTRACT(YEAR FROM CAST(SEARCHES.search_ts AS DATETIME)) <= 2019
    AND EXTRACT(YEAR FROM CAST(SEARCHES.search_ts AS DATETIME)) >= 2010
  GROUP BY
    SEARCHES.search_engine
)
SELECT
  _s2.search_engine AS engine,
  COALESCE(_s3.ndistinct_user_id, 0) AS n_users
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.search_engine = _s3.search_engine
ORDER BY
  _s2.search_engine COLLATE utf8mb4_bin
