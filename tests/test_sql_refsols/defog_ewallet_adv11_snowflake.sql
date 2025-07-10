WITH _S1 AS (
  SELECT
    COALESCE(
      SUM(
        DATEDIFF(SECOND, CAST(session_start_ts AS DATETIME), CAST(session_end_ts AS DATETIME))
      ),
      0
    ) AS TOTAL_DURATION,
    user_id AS USER_ID
  FROM MAIN.USER_SESSIONS
  WHERE
    session_end_ts < '2023-06-08' AND session_start_ts >= '2023-06-01'
  GROUP BY
    user_id
)
SELECT
  USERS.uid,
  _S1.TOTAL_DURATION AS total_duration
FROM MAIN.USERS AS USERS
JOIN _S1 AS _S1
  ON USERS.uid = _S1.USER_ID
ORDER BY
  TOTAL_DURATION DESC NULLS LAST
