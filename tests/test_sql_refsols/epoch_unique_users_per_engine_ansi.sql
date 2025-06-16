WITH _s2 AS (
  SELECT DISTINCT
    search_engine
  FROM searches
), _s3 AS (
  SELECT
    COUNT(DISTINCT users.user_id) AS agg_0,
    searches.search_engine
  FROM searches AS searches
  JOIN users AS users
    ON searches.search_user_id = users.user_id
  WHERE
    EXTRACT(YEAR FROM searches.search_ts) <= 2019
    AND EXTRACT(YEAR FROM searches.search_ts) >= 2010
  GROUP BY
    searches.search_engine
)
SELECT
  _s2.search_engine AS engine,
  COALESCE(_s3.agg_0, 0) AS n_users
FROM _s2 AS _s2
LEFT JOIN _s3 AS _s3
  ON _s2.search_engine = _s3.search_engine
ORDER BY
  _s2.search_engine
