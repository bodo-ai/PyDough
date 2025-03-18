SELECT
  agg_17 AS cust_region_name,
  agg_16 AS cust_region_key,
  agg_11 AS cust_nation_name,
  agg_10 AS cust_nation_key,
  customer_name,
  n_urgent_semi_domestic_rail_orders
FROM (
  SELECT
    agg_10,
    agg_11,
    agg_16,
    agg_17,
    customer_name,
    n_urgent_semi_domestic_rail_orders,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      ANY_VALUE(cust_nation_key) AS agg_10,
      ANY_VALUE(cust_nation_name) AS agg_11,
      ANY_VALUE(cust_region_key) AS agg_16,
      ANY_VALUE(cust_region_name) AS agg_17,
      ANY_VALUE(name_6) AS customer_name,
      ANY_VALUE(name_6) AS ordering_2,
      COUNT(DISTINCT order_key) AS n_urgent_semi_domestic_rail_orders,
      COUNT(DISTINCT order_key) AS ordering_1
    FROM (
      SELECT
        cust_nation_key,
        cust_nation_name,
        cust_region_key,
        cust_region_name,
        key,
        key_2,
        key_5,
        name_6,
        order_key
      FROM (
        SELECT
          _table_alias_14.key AS key,
          cust_nation_key,
          cust_nation_name,
          cust_region_key,
          cust_region_name,
          key_2,
          key_5,
          name_19,
          name_6,
          order_key
        FROM (
          SELECT
            cust_nation_key,
            cust_nation_name,
            cust_region_key,
            cust_region_name,
            key,
            key_2,
            key_5,
            name_6,
            order_key,
            supplier_key
          FROM (
            SELECT
              _table_alias_8.key AS key,
              cust_nation_key,
              cust_nation_name,
              cust_region_key,
              cust_region_name,
              key_2,
              key_5,
              name_12,
              name_6,
              order_key,
              supplier_key
            FROM (
              SELECT
                cust_nation_key,
                cust_nation_name,
                cust_region_key,
                cust_region_name,
                key,
                key_2,
                key_5,
                name_6,
                order_key,
                supplier_key
              FROM (
                SELECT
                  _table_alias_4.key AS key,
                  _table_alias_5.key AS key_8,
                  cust_nation_key,
                  cust_nation_name,
                  cust_region_key,
                  cust_region_name,
                  key_2,
                  key_5,
                  name_6
                FROM (
                  SELECT
                    _table_alias_2.key AS key,
                    _table_alias_3.key AS key_5,
                    name AS name_6,
                    cust_nation_key,
                    cust_nation_name,
                    cust_region_key,
                    cust_region_name,
                    key_2
                  FROM (
                    SELECT
                      _table_alias_0.key AS key,
                      _table_alias_1.key AS cust_nation_key,
                      _table_alias_1.key AS key_2,
                      name AS cust_nation_name,
                      cust_region_key,
                      cust_region_name
                    FROM (
                      SELECT
                        r_name AS cust_region_name,
                        r_regionkey AS cust_region_key,
                        r_regionkey AS key
                      FROM tpch.REGION
                    ) AS _table_alias_0
                    INNER JOIN (
                      SELECT
                        n_nationkey AS key,
                        n_name AS name,
                        n_regionkey AS region_key
                      FROM tpch.NATION
                    ) AS _table_alias_1
                      ON _table_alias_0.key = region_key
                  ) AS _table_alias_2
                  INNER JOIN (
                    SELECT
                      c_custkey AS key,
                      c_name AS name,
                      c_nationkey AS nation_key
                    FROM tpch.CUSTOMER
                  ) AS _table_alias_3
                    ON key_2 = nation_key
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
                      YEAR(order_date) = 1996
                    ) AND (
                      order_priority = '1-URGENT'
                    )
                ) AS _table_alias_5
                  ON key_5 = customer_key
              )
              INNER JOIN (
                SELECT
                  order_key,
                  supplier_key
                FROM (
                  SELECT
                    l_orderkey AS order_key,
                    l_shipmode AS ship_mode,
                    l_suppkey AS supplier_key
                  FROM tpch.LINEITEM
                )
                WHERE
                  ship_mode = 'RAIL'
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
            name_12 <> cust_nation_name
        ) AS _table_alias_14
        LEFT JOIN (
          SELECT
            _table_alias_12.key AS key,
            name AS name_19
          FROM (
            SELECT
              _table_alias_10.key AS key,
              region_key
            FROM (
              SELECT
                s_suppkey AS key,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_10
            INNER JOIN (
              SELECT
                n_nationkey AS key,
                n_regionkey AS region_key
              FROM tpch.NATION
            ) AS _table_alias_11
              ON nation_key = _table_alias_11.key
          ) AS _table_alias_12
          INNER JOIN (
            SELECT
              r_regionkey AS key,
              r_name AS name
            FROM tpch.REGION
          ) AS _table_alias_13
            ON region_key = _table_alias_13.key
        ) AS _table_alias_15
          ON supplier_key = _table_alias_15.key
      )
      WHERE
        name_19 = cust_region_name
    )
    GROUP BY
      key_5,
      key_2,
      key
  )
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 5
)
ORDER BY
  ordering_1 DESC,
  ordering_2
