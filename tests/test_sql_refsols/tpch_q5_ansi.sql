SELECT
  N_NAME,
  REVENUE
FROM (
  SELECT
    COALESCE(agg_0, 0) AS REVENUE,
    COALESCE(agg_0, 0) AS ordering_1,
    agg_3 AS N_NAME
  FROM (
    SELECT
      ANY_VALUE(name) AS agg_3,
      SUM(value) AS agg_0,
      key
    FROM (
      SELECT
        extended_price * (
          1 - discount
        ) AS value,
        key,
        name
      FROM (
        SELECT
          discount,
          extended_price,
          key,
          name
        FROM (
          SELECT
            _table_alias_8.key AS key,
            discount,
            extended_price,
            name,
            name_12,
            nation_name
          FROM (
            SELECT
              discount,
              extended_price,
              key,
              name,
              nation_name,
              supplier_key
            FROM (
              SELECT
                _table_alias_4.key AS key,
                _table_alias_5.key AS key_8,
                name,
                nation_name
              FROM (
                SELECT
                  _table_alias_2.key AS key,
                  _table_alias_3.key AS key_5,
                  name,
                  nation_name
                FROM (
                  SELECT
                    _table_alias_0.key AS key,
                    name,
                    nation_name
                  FROM (
                    SELECT
                      n_name AS name,
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
                      name = 'ASIA'
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
                    o_orderkey AS key
                  FROM tpch.ORDERS
                )
                WHERE
                  (
                    order_date < CAST('1995-01-01' AS DATE)
                  )
                  AND (
                    order_date >= CAST('1994-01-01' AS DATE)
                  )
              ) AS _table_alias_5
                ON key_5 = customer_key
            )
            INNER JOIN (
              SELECT
                l_discount AS discount,
                l_extendedprice AS extended_price,
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
    )
    GROUP BY
      key
  )
)
ORDER BY
  ordering_1 DESC
