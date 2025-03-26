WITH _table_alias_0 AS (
  SELECT
    users.uid AS uid
  FROM main.users AS users
), _table_alias_1 AS (
  SELECT
    SUM(
      DATEDIFF(
        CAST(user_sessions.session_end_ts AS DATETIME),
        CAST(user_sessions.session_start_ts AS DATETIME),
        SECOND
      )
    ) AS agg_0,
    user_sessions.user_id AS user_id
  FROM main.user_sessions AS user_sessions
  WHERE
    user_sessions.session_end_ts < '2023-06-08'
    AND user_sessions.session_start_ts >= '2023-06-01'
  GROUP BY
    user_sessions.user_id
)
SELECT
  _table_alias_0.uid AS uid,
  COALESCE(_table_alias_1.agg_0, 0) AS total_duration
FROM _table_alias_0 AS _table_alias_0
JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.uid = _table_alias_1.user_id
ORDER BY
  COALESCE(_table_alias_1.agg_0, 0) DESC
