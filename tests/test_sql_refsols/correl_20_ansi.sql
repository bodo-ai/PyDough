SELECT
  COUNT() AS n
FROM (
  SELECT
    account_balance
  FROM (
    SELECT
      name_15 = source_nation_name AS domestic,
      account_balance
    FROM (
      SELECT
        name AS name_15,
        account_balance,
        source_nation_name
      FROM (
        SELECT
          nation_key AS nation_key_11,
          account_balance,
          source_nation_name
        FROM (
          SELECT
            source_nation_name,
            supplier_key
          FROM (
            SELECT
              key AS key_5,
              source_nation_name
            FROM (
              SELECT
                _table_alias_1.key AS key_2,
                source_nation_name
              FROM (
                SELECT
                  n_name AS source_nation_name,
                  n_nationkey AS key
                FROM tpch.NATION
              ) AS _table_alias_0
              INNER JOIN (
                SELECT
                  c_custkey AS key,
                  c_nationkey AS nation_key
                FROM tpch.CUSTOMER
              ) AS _table_alias_1
                ON _table_alias_0.key = nation_key
            )
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
                  MONTH(order_date) = 6
                ) AND (
                  YEAR(order_date) = 1998
                )
            )
              ON key_2 = customer_key
          )
          INNER JOIN (
            SELECT
              l_orderkey AS order_key,
              l_suppkey AS supplier_key
            FROM tpch.LINEITEM
          )
            ON key_5 = order_key
        )
        INNER JOIN (
          SELECT
            s_acctbal AS account_balance,
            s_suppkey AS key,
            s_nationkey AS nation_key
          FROM tpch.SUPPLIER
        )
          ON supplier_key = key
      )
      INNER JOIN (
        SELECT
          n_nationkey AS key,
          n_name AS name
        FROM tpch.NATION
      )
        ON nation_key_11 = key
    )
  )
  WHERE
    domestic
)
