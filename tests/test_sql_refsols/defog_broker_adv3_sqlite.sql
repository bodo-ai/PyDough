SELECT
  name,
  success_rate
FROM (
  SELECT
    CAST((
      100.0 * COALESCE(agg_1, 0)
    ) AS REAL) / COALESCE(agg_0, 0) AS ordering_2,
    CAST((
      100.0 * COALESCE(agg_1, 0)
    ) AS REAL) / COALESCE(agg_0, 0) AS success_rate,
    name
  FROM (
    SELECT
      agg_0,
      agg_1,
      name
    FROM (
      SELECT
        sbCustId AS _id,
        sbCustName AS name
      FROM main.sbCustomer
    )
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        SUM(status = 'success') AS agg_1,
        customer_id
      FROM (
        SELECT
          sbTxCustId AS customer_id,
          sbTxStatus AS status
        FROM main.sbTransaction
      )
      GROUP BY
        customer_id
    )
      ON _id = customer_id
    WHERE
      COALESCE(agg_0, 0) >= 5
  )
)
ORDER BY
  ordering_2
