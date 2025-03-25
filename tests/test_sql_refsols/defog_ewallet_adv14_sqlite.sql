SELECT
  CAST(COALESCE(agg_0, 0) AS REAL) / agg_1 AS _expr0
FROM (
  SELECT
    COUNT() AS agg_1,
    SUM(status = 'success') AS agg_0
  FROM (
    SELECT
      status
    FROM (
      SELECT
        created_at,
        status
      FROM main.wallet_transactions_daily
    ) AS _t2
    WHERE
      (
        (
          CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', created_at) AS INTEGER)
        ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', created_at) AS INTEGER)
      ) = 1
  ) AS _t1
) AS _t0
