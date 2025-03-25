SELECT
  name,
  total_amount
FROM (
  SELECT
    name,
    ordering_1,
    total_amount
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS ordering_1,
      COALESCE(agg_0, 0) AS total_amount,
      name
    FROM (
      SELECT
        agg_0,
        name
      FROM (
        SELECT
          sbCustId AS _id,
          sbCustName AS name
        FROM main.sbCustomer
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          SUM(amount) AS agg_0,
          customer_id
        FROM (
          SELECT
            sbTxAmount AS amount,
            sbTxCustId AS customer_id
          FROM main.sbTransaction
        ) AS _t3
        GROUP BY
          customer_id
      ) AS _table_alias_1
        ON _id = customer_id
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC
  LIMIT 5
) AS _t0
ORDER BY
  ordering_1 DESC
