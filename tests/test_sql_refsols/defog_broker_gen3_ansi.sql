SELECT
  _id AS cust_id,
  DATEDIFF(agg_0, join_date, SECOND) / 86400.0 AS DaysFromJoinToFirstTransaction
FROM (
  SELECT
    _id,
    agg_0,
    join_date
  FROM (
    SELECT
      sbCustId AS _id,
      sbCustJoinDate AS join_date
    FROM main.sbCustomer
  ) AS _table_alias_0
  INNER JOIN (
    SELECT
      MIN(date_time) AS agg_0,
      customer_id
    FROM (
      SELECT
        sbTxCustId AS customer_id,
        sbTxDateTime AS date_time
      FROM main.sbTransaction
    ) AS _t1
    GROUP BY
      customer_id
  ) AS _table_alias_1
    ON _id = customer_id
) AS _t0
