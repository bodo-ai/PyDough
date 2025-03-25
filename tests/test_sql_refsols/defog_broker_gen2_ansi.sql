SELECT
  COUNT(customer_id) AS transaction_count
FROM (
  SELECT
    customer_id
  FROM (
    SELECT
      sbTxCustId AS customer_id
    FROM main.sbTransaction
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      _id
    FROM (
      SELECT
        sbCustId AS _id,
        sbCustJoinDate AS join_date
      FROM main.sbCustomer
    ) AS _t1
    WHERE
      join_date >= DATE_ADD(CURRENT_TIMESTAMP(), -70, 'DAY')
  ) AS _table_alias_1
    ON customer_id = _id
) AS _t0
