SELECT
  COUNT() AS TUC
FROM (
  SELECT
    device_id
  FROM (
    SELECT
      device_id,
      session_start_ts
    FROM main.user_sessions
  ) AS _t1
  WHERE
    session_start_ts >= DATE_TRUNC('DAY', DATE_ADD(CURRENT_TIMESTAMP(), -1, 'MONTH'))
) AS _t0
