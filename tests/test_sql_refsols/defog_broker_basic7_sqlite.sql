SELECT
  status,
  num_transactions
FROM (
  SELECT
    num_transactions,
    ordering_1,
    status
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_transactions,
      COALESCE(agg_0, 0) AS ordering_1,
      status
    FROM (
      SELECT
        COUNT() AS agg_0,
        status
      FROM (
        SELECT
          sbTxStatus AS status
        FROM main.sbTransaction
      ) AS _t3
      GROUP BY
        status
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 3
) AS _t0
ORDER BY
  ordering_1 DESC
