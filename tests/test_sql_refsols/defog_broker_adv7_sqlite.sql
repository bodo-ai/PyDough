SELECT
  month,
  COALESCE(agg_1, 0) AS customer_signups,
  agg_0 AS avg_tx_amount
FROM (
  SELECT
    _table_alias_2.month AS month,
    agg_0,
    agg_1
  FROM (
    SELECT
      COUNT() AS agg_1,
      month
    FROM (
      SELECT
        CONCAT_WS(
          '-',
          CAST(STRFTIME('%Y', join_date) AS INTEGER),
          CASE
            WHEN LENGTH(CAST(STRFTIME('%m', join_date) AS INTEGER)) >= 2
            THEN SUBSTRING(CAST(STRFTIME('%m', join_date) AS INTEGER), 1, 2)
            ELSE SUBSTRING('00' || CAST(STRFTIME('%m', join_date) AS INTEGER), (
              2 * -1
            ))
          END
        ) AS month
      FROM (
        SELECT
          join_date
        FROM (
          SELECT
            sbCustJoinDate AS join_date
          FROM main.sbCustomer
        ) AS _t3
        WHERE
          (
            join_date < DATE('now', 'start of month')
          )
          AND (
            join_date >= DATE(DATETIME('now', '-6 month'), 'start of month')
          )
      ) AS _t2
    ) AS _t1
    GROUP BY
      month
  ) AS _table_alias_2
  LEFT JOIN (
    SELECT
      AVG(amount) AS agg_0,
      month
    FROM (
      SELECT
        amount,
        month
      FROM (
        SELECT
          amount,
          date_time,
          join_month,
          join_year,
          month
        FROM (
          SELECT
            CAST(STRFTIME('%Y', join_date) AS INTEGER) AS join_year,
            CAST(STRFTIME('%m', join_date) AS INTEGER) AS join_month,
            CONCAT_WS(
              '-',
              CAST(STRFTIME('%Y', join_date) AS INTEGER),
              CASE
                WHEN LENGTH(CAST(STRFTIME('%m', join_date) AS INTEGER)) >= 2
                THEN SUBSTRING(CAST(STRFTIME('%m', join_date) AS INTEGER), 1, 2)
                ELSE SUBSTRING('00' || CAST(STRFTIME('%m', join_date) AS INTEGER), (
                  2 * -1
                ))
              END
            ) AS month,
            _id
          FROM (
            SELECT
              _id,
              join_date
            FROM (
              SELECT
                sbCustId AS _id,
                sbCustJoinDate AS join_date
              FROM main.sbCustomer
            ) AS _t7
            WHERE
              (
                join_date < DATE('now', 'start of month')
              )
              AND (
                join_date >= DATE(DATETIME('now', '-6 month'), 'start of month')
              )
          ) AS _t6
        ) AS _table_alias_0
        INNER JOIN (
          SELECT
            sbTxAmount AS amount,
            sbTxCustId AS customer_id,
            sbTxDateTime AS date_time
          FROM main.sbTransaction
        ) AS _table_alias_1
          ON _id = customer_id
      ) AS _t5
      WHERE
        (
          CAST(STRFTIME('%m', date_time) AS INTEGER) = join_month
        )
        AND (
          CAST(STRFTIME('%Y', date_time) AS INTEGER) = join_year
        )
    ) AS _t4
    GROUP BY
      month
  ) AS _table_alias_3
    ON _table_alias_2.month = _table_alias_3.month
) AS _t0
