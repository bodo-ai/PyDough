SELECT
  supplier_name,
  n_super_cust
FROM (
  SELECT
    n_super_cust,
    ordering_1,
    supplier_name
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS n_super_cust,
      COALESCE(agg_0, 0) AS ordering_1,
      agg_5 AS supplier_name
    FROM (
      SELECT
        MAX(name) AS agg_5,
        COUNT() AS agg_0,
        key
      FROM (
        SELECT
          key,
          name
        FROM (
          SELECT
            account_balance,
            acctbal,
            key,
            name
          FROM (
            SELECT
              _table_alias_0.key AS key,
              _table_alias_1.key AS key_2,
              account_balance,
              name
            FROM (
              SELECT
                s_acctbal AS account_balance,
                s_suppkey AS key,
                s_name AS name,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_0
            INNER JOIN (
              SELECT
                n_nationkey AS key
              FROM tpch.NATION
            ) AS _table_alias_1
              ON nation_key = _table_alias_1.key
          )
          INNER JOIN (
            SELECT
              c_acctbal AS acctbal,
              c_nationkey AS nation_key
            FROM tpch.CUSTOMER
          )
            ON key_2 = nation_key
        )
        WHERE
          acctbal > account_balance
      )
      GROUP BY
        key
    )
  )
  ORDER BY
    ordering_1 DESC
  LIMIT 5
)
ORDER BY
  ordering_1 DESC
