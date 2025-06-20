WITH _s5 AS (
  SELECT DISTINCT
    search_engine
  FROM searches
), _s6 AS (
  SELECT
    COUNT(DISTINCT _s2.user_id) AS agg_0,
    _s1.search_engine
  FROM searches AS _s1
  JOIN users AS _s2
    ON _s1.search_user_id = _s2.user_id
  WHERE
    EXTRACT(YEAR FROM _s1.search_ts) <= 2019
    AND EXTRACT(YEAR FROM _s1.search_ts) >= 2010
  GROUP BY
    _s1.search_engine
)
SELECT
  _s5.search_engine AS engine,
  COALESCE(_s6.agg_0, 0) AS n_users
FROM _s5 AS _s5
LEFT JOIN _s6 AS _s6
  ON _s5.search_engine = _s6.search_engine
ORDER BY
  engine
