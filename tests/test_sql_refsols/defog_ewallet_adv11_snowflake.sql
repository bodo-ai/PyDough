WITH _T0 AS (
  SELECT
    SUM(DATEDIFF(SECOND, session_start_ts, session_end_ts)) AS AGG_0,
    user_id AS USER_ID
  FROM MAIN.USER_SESSIONS
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  USERS.uid,
  COALESCE(_T0.AGG_0, 0) AS total_duration
FROM MAIN.USERS AS USERS
JOIN _T0 AS _T0
  ON USERS.uid = _T0.USER_ID
ORDER BY
  TOTAL_DURATION DESC NULLS LAST
