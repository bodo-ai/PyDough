WITH _s2 AS (
  SELECT DISTINCT
    search_engine
  FROM searches
), _s3 AS (
  SELECT
    searches.search_engine,
    COUNT(DISTINCT users.user_id) AS ndistinct_user_id
  FROM searches AS searches
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  WHERE
    EXTRACT(YEAR FROM CAST(searches.search_ts AS DATETIME)) <= 2019
    AND EXTRACT(YEAR FROM CAST(searches.search_ts AS DATETIME)) >= 2010
  GROUP BY
    1
)
SELECT
  _s2.search_engine AS engine,
  _s3.ndistinct_user_id AS n_users
FROM _s2 AS _s2
JOIN _s3 AS _s3
  ON _s2.search_engine = _s3.search_engine
ORDER BY
  1
