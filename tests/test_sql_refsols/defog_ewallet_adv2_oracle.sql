SELECT
  TRUNC(
    CAST(CAST(NOTIFICATIONS.created_at AS DATE) - MOD((
      TO_CHAR(CAST(NOTIFICATIONS.created_at AS DATE), 'D') + 5
    ), 7) AS DATE),
    'DD'
  ) AS week,
  COUNT(*) AS num_notifs,
  SUM((
    MOD((
      TO_CHAR(NOTIFICATIONS.created_at, 'D') + 5
    ), 7)
  ) IN (5, 6)) AS weekend_notifs
FROM MAIN.NOTIFICATIONS NOTIFICATIONS
JOIN MAIN.USERS USERS
  ON NOTIFICATIONS.user_id = USERS.uid AND USERS.country IN ('US', 'CA')
WHERE
  NOTIFICATIONS.created_at < TRUNC(
    SYS_EXTRACT_UTC(SYSTIMESTAMP) - MOD((
      TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'D') + 5
    ), 7),
    'DD'
  )
  AND NOTIFICATIONS.created_at >= (
    TRUNC(
      SYS_EXTRACT_UTC(SYSTIMESTAMP) - MOD((
        TO_CHAR(SYS_EXTRACT_UTC(SYSTIMESTAMP), 'D') + 5
      ), 7),
      'DD'
    ) - NUMTODSINTERVAL(21, 'DAY')
  )
GROUP BY
  TRUNC(
    CAST(CAST(NOTIFICATIONS.created_at AS DATE) - MOD((
      TO_CHAR(CAST(NOTIFICATIONS.created_at AS DATE), 'D') + 5
    ), 7) AS DATE),
    'DD'
  )
