SELECT
  nation_name,
  n_selected_purchases
FROM (
  SELECT
    COALESCE(agg_0, 0) AS n_selected_purchases,
    agg_4 AS nation_name,
    agg_4 AS ordering_1
  FROM (
    SELECT
      ANY_VALUE(nation_name) AS agg_4,
      COUNT() AS agg_0,
      key
    FROM (
      SELECT
        key,
        nation_name
      FROM (
        SELECT
          _table_alias_8.key AS key,
          name_12,
          nation_name
        FROM (
          SELECT
            key,
            nation_name,
            supplier_key
          FROM (
            SELECT
              _table_alias_4.key AS key,
              _table_alias_5.key AS key_8,
              nation_name
            FROM (
              SELECT
                _table_alias_2.key AS key,
                _table_alias_3.key AS key_5,
                nation_name
              FROM (
                SELECT
                  _table_alias_0.key AS key,
                  nation_name
                FROM (
                  SELECT
                    n_name AS nation_name,
                    n_nationkey AS key,
                    n_regionkey AS region_key
                  FROM tpch.NATION
                ) AS _table_alias_0
                INNER JOIN (
                  SELECT
                    key
                  FROM (
                    SELECT
                      r_name AS name,
                      r_regionkey AS key
                    FROM tpch.REGION
                  )
                  WHERE
                    name = 'EUROPE'
                ) AS _table_alias_1
                  ON region_key = _table_alias_1.key
              ) AS _table_alias_2
              INNER JOIN (
                SELECT
                  c_custkey AS key,
                  c_nationkey AS nation_key
                FROM tpch.CUSTOMER
              ) AS _table_alias_3
                ON _table_alias_2.key = nation_key
            ) AS _table_alias_4
            INNER JOIN (
              SELECT
                customer_key,
                key
              FROM (
                SELECT
                  o_custkey AS customer_key,
                  o_orderdate AS order_date,
                  o_orderkey AS key,
                  o_orderpriority AS order_priority
                FROM tpch.ORDERS
              )
              WHERE
                (
                  YEAR(order_date) = 1994
                ) AND (
                  order_priority = '1-URGENT'
                )
            ) AS _table_alias_5
              ON key_5 = customer_key
          )
          INNER JOIN (
            SELECT
              l_orderkey AS order_key,
              l_suppkey AS supplier_key
            FROM tpch.LINEITEM
          )
            ON key_8 = order_key
        ) AS _table_alias_8
        LEFT JOIN (
          SELECT
            _table_alias_6.key AS key,
            name AS name_12
          FROM (
            SELECT
              s_suppkey AS key,
              s_nationkey AS nation_key
            FROM tpch.SUPPLIER
          ) AS _table_alias_6
          INNER JOIN (
            SELECT
              n_nationkey AS key,
              n_name AS name
            FROM tpch.NATION
          ) AS _table_alias_7
            ON nation_key = _table_alias_7.key
        ) AS _table_alias_9
          ON supplier_key = _table_alias_9.key
      )
      WHERE
        name_12 = nation_name
    )
    GROUP BY
      key
  )
)
ORDER BY
  ordering_1
