WITH _s0 AS (
  SELECT DISTINCT
    search_engine
  FROM searches
), _s1 AS (
  SELECT
    search_engine,
    COUNT(DISTINCT search_user_id) AS ndistinct_search_user_id
  FROM searches
  WHERE
    EXTRACT(YEAR FROM CAST(search_ts AS TIMESTAMP)) <= 2019
    AND EXTRACT(YEAR FROM CAST(search_ts AS TIMESTAMP)) >= 2010
  GROUP BY
    1
)
SELECT
  _s0.search_engine AS engine,
  COALESCE(_s1.ndistinct_search_user_id, 0) AS n_users
FROM _s0 AS _s0
LEFT JOIN _s1 AS _s1
  ON _s0.search_engine = _s1.search_engine
ORDER BY
  1 NULLS FIRST
