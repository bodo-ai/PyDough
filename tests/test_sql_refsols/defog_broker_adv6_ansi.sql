SELECT
  name,
  COALESCE(agg_1, 0) AS num_tx,
  COALESCE(agg_2, 0) AS total_amount,
  RANK() OVER (ORDER BY COALESCE(agg_0, 0) DESC NULLS FIRST) AS cust_rank
FROM (
  SELECT
    agg_0,
    agg_1,
    agg_2,
    name
  FROM (
    SELECT
      sbCustId AS _id,
      sbCustName AS name
    FROM main.sbCustomer
  )
  INNER JOIN (
    SELECT
      COUNT() AS agg_1,
      SUM(amount) AS agg_0,
      SUM(amount) AS agg_2,
      customer_id
    FROM (
      SELECT
        sbTxAmount AS amount,
        sbTxCustId AS customer_id
      FROM main.sbTransaction
    )
    GROUP BY
      customer_id
  )
    ON _id = customer_id
)
