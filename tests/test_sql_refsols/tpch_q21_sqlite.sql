SELECT
  S_NAME,
  NUMWAIT
FROM (
  SELECT
    COALESCE(agg_0, 0) AS NUMWAIT,
    name AS S_NAME
  FROM (
    SELECT
      agg_0,
      name
    FROM (
      SELECT
        _table_alias_0.key AS key,
        name
      FROM (
        SELECT
          s_suppkey AS key,
          s_name AS name,
          s_nationkey AS nation_key
        FROM tpch.SUPPLIER
      ) AS _table_alias_0
      INNER JOIN (
        SELECT
          key
        FROM (
          SELECT
            n_name AS name,
            n_nationkey AS key
          FROM tpch.NATION
        )
        WHERE
          name = 'SAUDI ARABIA'
      ) AS _table_alias_1
        ON nation_key = _table_alias_1.key
    )
    LEFT JOIN (
      SELECT
        COUNT() AS agg_0,
        supplier_key
      FROM (
        SELECT
          supplier_key
        FROM (
          SELECT
            key,
            original_key,
            supplier_key
          FROM (
            SELECT
              key,
              original_key,
              supplier_key
            FROM (
              SELECT
                supplier_key AS original_key,
                order_key,
                supplier_key
              FROM (
                SELECT
                  l_commitdate AS commit_date,
                  l_orderkey AS order_key,
                  l_receiptdate AS receipt_date,
                  l_suppkey AS supplier_key
                FROM tpch.LINEITEM
              )
              WHERE
                receipt_date > commit_date
            )
            INNER JOIN (
              SELECT
                key
              FROM (
                SELECT
                  o_orderkey AS key,
                  o_orderstatus AS order_status
                FROM tpch.ORDERS
              )
              WHERE
                order_status = 'F'
            )
              ON order_key = key
          ) AS _table_alias_3
          WHERE
            EXISTS(
              SELECT
                1
              FROM (
                SELECT
                  order_key
                FROM (
                  SELECT
                    l_orderkey AS order_key,
                    l_suppkey AS supplier_key
                  FROM tpch.LINEITEM
                )
                WHERE
                  supplier_key <> _table_alias_3.original_key
              )
              WHERE
                key = order_key
            )
        ) AS _table_alias_2
        WHERE
          NOT EXISTS(
            SELECT
              1
            FROM (
              SELECT
                order_key
              FROM (
                SELECT
                  l_commitdate AS commit_date,
                  l_orderkey AS order_key,
                  l_receiptdate AS receipt_date,
                  l_suppkey AS supplier_key
                FROM tpch.LINEITEM
              )
              WHERE
                (
                  supplier_key <> _table_alias_2.original_key
                )
                AND (
                  receipt_date > commit_date
                )
            )
            WHERE
              key = order_key
          )
      )
      GROUP BY
        supplier_key
    )
      ON key = supplier_key
  )
)
ORDER BY
  NUMWAIT DESC,
  S_NAME
LIMIT 10
