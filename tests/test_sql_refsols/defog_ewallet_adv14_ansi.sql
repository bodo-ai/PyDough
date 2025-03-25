SELECT
  COALESCE(agg_0, 0) / agg_1 AS _expr0
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
      DATEDIFF(CURRENT_TIMESTAMP(), created_at, MONTH) = 1
  ) AS _t1
) AS _t0
