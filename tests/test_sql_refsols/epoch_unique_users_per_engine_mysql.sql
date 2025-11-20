WITH _s2 AS (
  SELECT DISTINCT
    search_engine
  FROM SEARCHES
), _s3 AS (
  SELECT
    SEARCHES.search_engine,
    COUNT(DISTINCT USERS.user_id) AS ndistinct_userid
  FROM SEARCHES AS SEARCHES
  JOIN USERS AS USERS
    ON SEARCHES.search_user_id = USERS.user_id
  WHERE
    EXTRACT(YEAR FROM CAST(SEARCHES.search_ts AS DATETIME)) <= 2019
    AND EXTRACT(YEAR FROM CAST(SEARCHES.search_ts AS DATETIME)) >= 2010
  GROUP BY
    1
)
SELECT
  _s2.search_engine COLLATE utf8mb4_bin AS engine,
  COALESCE(_s3.ndistinct_userid, 0) AS n_users
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.search_engine = _s3.search_engine
ORDER BY
  1
