SELECT
  _id,
  name,
  num_transactions
FROM (
  SELECT
    _id,
    name,
    num_transactions,
    ordering_1
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS num_transactions,
      COALESCE(agg_0, 0) AS ordering_1,
      _id,
      name
    FROM (
      SELECT
        _table_alias_0._id AS _id,
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
          COUNT() AS agg_0,
          _id
        FROM (
          SELECT
            _id
          FROM (
            SELECT
              _id,
              date_time,
              join_month,
              join_year
            FROM (
              SELECT
                EXTRACT(MONTH FROM join_date) AS join_month,
                EXTRACT(YEAR FROM join_date) AS join_year,
                _id
              FROM (
                SELECT
                  sbCustId AS _id,
                  sbCustJoinDate AS join_date
                FROM main.sbCustomer
              )
            )
            INNER JOIN (
              SELECT
                sbTxCustId AS customer_id,
                sbTxDateTime AS date_time
              FROM main.sbTransaction
            )
              ON _id = customer_id
          )
          WHERE
            (
              EXTRACT(MONTH FROM date_time) = join_month
            )
            AND (
              EXTRACT(YEAR FROM date_time) = join_year
            )
        )
        GROUP BY
          _id
      ) AS _table_alias_1
        ON _table_alias_0._id = _table_alias_1._id
    )
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 1
)
ORDER BY
  ordering_1 DESC
