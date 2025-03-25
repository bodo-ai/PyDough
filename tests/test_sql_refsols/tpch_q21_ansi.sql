SELECT
  S_NAME,
  NUMWAIT
FROM (
  SELECT
    NUMWAIT,
    S_NAME,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS NUMWAIT,
      COALESCE(agg_0, 0) AS ordering_1,
      name AS S_NAME,
      name AS ordering_2
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
          ) AS _t3
          WHERE
            name = 'SAUDI ARABIA'
        ) AS _table_alias_1
          ON nation_key = _table_alias_1.key
      ) AS _table_alias_8
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
                ) AS _t5
                WHERE
                  receipt_date > commit_date
              ) AS _table_alias_4
              INNER JOIN (
                SELECT
                  key
                FROM (
                  SELECT
                    o_orderkey AS key,
                    o_orderstatus AS order_status
                  FROM tpch.ORDERS
                ) AS _t6
                WHERE
                  order_status = 'F'
              ) AS _table_alias_5
                ON order_key = key
            ) AS _table_alias_3
            SEMI JOIN (
              SELECT
                order_key
              FROM (
                SELECT
                  l_orderkey AS order_key,
                  l_suppkey AS supplier_key
                FROM tpch.LINEITEM
              ) AS _t7
              WHERE
                supplier_key <> _table_alias_3.original_key
            ) AS _table_alias_6
              ON key = order_key
          ) AS _table_alias_2
          ANTI JOIN (
            SELECT
              order_key
            FROM (
              SELECT
                l_commitdate AS commit_date,
                l_orderkey AS order_key,
                l_receiptdate AS receipt_date,
                l_suppkey AS supplier_key
              FROM tpch.LINEITEM
            ) AS _t8
            WHERE
              (
                supplier_key <> _table_alias_2.original_key
              )
              AND (
                receipt_date > commit_date
              )
          ) AS _table_alias_7
            ON key = order_key
        ) AS _t4
        GROUP BY
          supplier_key
      ) AS _table_alias_9
        ON key = supplier_key
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1 DESC,
  ordering_2
