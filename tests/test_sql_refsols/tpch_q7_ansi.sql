SELECT
  SUPP_NATION,
  CUST_NATION,
  L_YEAR,
  REVENUE
FROM (
  SELECT
    COALESCE(agg_0, 0) AS REVENUE,
    cust_nation AS CUST_NATION,
    cust_nation AS ordering_2,
    l_year AS L_YEAR,
    l_year AS ordering_3,
    supp_nation AS SUPP_NATION,
    supp_nation AS ordering_1
  FROM (
    SELECT
      SUM(volume) AS agg_0,
      cust_nation,
      l_year,
      supp_nation
    FROM (
      SELECT
        EXTRACT(YEAR FROM ship_date) AS l_year,
        name_3 AS supp_nation,
        name_8 AS cust_nation,
        extended_price * (
          1 - discount
        ) AS volume
      FROM (
        SELECT
          discount,
          extended_price,
          name_3,
          name_8,
          ship_date
        FROM (
          SELECT
            discount,
            extended_price,
            name_3,
            order_key,
            ship_date
          FROM (
            SELECT
              discount,
              extended_price,
              order_key,
              ship_date,
              supplier_key
            FROM (
              SELECT
                l_discount AS discount,
                l_extendedprice AS extended_price,
                l_orderkey AS order_key,
                l_shipdate AS ship_date,
                l_suppkey AS supplier_key
              FROM tpch.LINEITEM
            )
            WHERE
              (
                ship_date <= CAST('1996-12-31' AS DATE)
              )
              AND (
                ship_date >= CAST('1995-01-01' AS DATE)
              )
          )
          LEFT JOIN (
            SELECT
              _table_alias_0.key AS key,
              name AS name_3
            FROM (
              SELECT
                s_suppkey AS key,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_0
            INNER JOIN (
              SELECT
                n_nationkey AS key,
                n_name AS name
              FROM tpch.NATION
            ) AS _table_alias_1
              ON nation_key = _table_alias_1.key
          )
            ON supplier_key = key
        )
        LEFT JOIN (
          SELECT
            _table_alias_4.key AS key,
            name AS name_8
          FROM (
            SELECT
              _table_alias_2.key AS key,
              nation_key
            FROM (
              SELECT
                o_custkey AS customer_key,
                o_orderkey AS key
              FROM tpch.ORDERS
            ) AS _table_alias_2
            INNER JOIN (
              SELECT
                c_custkey AS key,
                c_nationkey AS nation_key
              FROM tpch.CUSTOMER
            ) AS _table_alias_3
              ON customer_key = _table_alias_3.key
          ) AS _table_alias_4
          INNER JOIN (
            SELECT
              n_nationkey AS key,
              n_name AS name
            FROM tpch.NATION
          ) AS _table_alias_5
            ON nation_key = _table_alias_5.key
        )
          ON order_key = key
        WHERE
          (
            (
              name_3 = 'FRANCE'
            ) AND (
              name_8 = 'GERMANY'
            )
          )
          OR (
            (
              name_3 = 'GERMANY'
            ) AND (
              name_8 = 'FRANCE'
            )
          )
      )
    )
    GROUP BY
      cust_nation,
      l_year,
      supp_nation
  )
)
ORDER BY
  ordering_1,
  ordering_2,
  ordering_3
