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
      agg_0,
      agg_4
    FROM (
      SELECT
        ANY_VALUE(nation_name) AS agg_4,
        ANY_VALUE(region_key) AS agg_5,
        COUNT() AS agg_0,
        key
      FROM (
        SELECT
          key,
          nation_name,
          region_key
        FROM (
          SELECT
            _table_alias_6.key AS key,
            name_9,
            nation_name,
            region_key
          FROM (
            SELECT
              key,
              nation_name,
              region_key,
              supplier_key
            FROM (
              SELECT
                _table_alias_2.key AS key,
                _table_alias_3.key AS key_5,
                nation_name,
                region_key
              FROM (
                SELECT
                  _table_alias_0.key AS key,
                  _table_alias_1.key AS key_2,
                  nation_name,
                  region_key
                FROM (
                  SELECT
                    n_name AS nation_name,
                    n_nationkey AS key,
                    n_regionkey AS region_key
                  FROM tpch.NATION
                ) AS _table_alias_0
                INNER JOIN (
                  SELECT
                    c_custkey AS key,
                    c_nationkey AS nation_key
                  FROM tpch.CUSTOMER
                ) AS _table_alias_1
                  ON _table_alias_0.key = nation_key
              ) AS _table_alias_2
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
              ) AS _table_alias_3
                ON key_2 = customer_key
            )
            INNER JOIN (
              SELECT
                l_orderkey AS order_key,
                l_suppkey AS supplier_key
              FROM tpch.LINEITEM
            )
              ON key_5 = order_key
          ) AS _table_alias_6
          LEFT JOIN (
            SELECT
              _table_alias_4.key AS key,
              name AS name_9
            FROM (
              SELECT
                s_suppkey AS key,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_4
            INNER JOIN (
              SELECT
                n_nationkey AS key,
                n_name AS name
              FROM tpch.NATION
            ) AS _table_alias_5
              ON nation_key = _table_alias_5.key
          ) AS _table_alias_7
            ON supplier_key = _table_alias_7.key
        )
        WHERE
          name_9 = nation_name
      )
      GROUP BY
        key
    ) AS _table_alias_8
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
    ) AS _table_alias_9
      ON agg_5 = _table_alias_9.key
  )
)
ORDER BY
  ordering_1
