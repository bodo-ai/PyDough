SELECT
  DATE('now', 'start of year', '5 month', '-1 day') AS d1,
  DATETIME(DATE('now', 'start of month'), '24 hour') AS d2,
  DATETIME(DATE('now', 'start of day'), '12 hour', '-150 minute', '2 second') AS d3
FROM (VALUES
  (NULL)) AS _q_0
